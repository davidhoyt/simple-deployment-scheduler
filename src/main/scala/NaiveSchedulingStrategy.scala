
/**
 * Solution
 * ---------
 * This approach starts by producing slots of time starting at a given time and then are expanded by a duration
 * that represents the amount of time a deployment is expected to take. This is analagous to the wait time between
 * deployments. The production of slots looks like the following:
 *
 *     <starting time> + 00:00 - 01:00
 *     <starting time> + 01:00 - 02:00
 *     <starting time> + 02:00 - 03:00
 *                ...
 *     <starting time> + 23:00 - 00:00
 *
 * After slots are created, combinations of those slots are generated and assigned to regions. The slot -> region
 * pair is called a "plan" and each plan is evaluated by running it through a cost function. Plans resemble the
 * following:
 *
 *     Plan #1: (US West -> 00:00 - 01:00) (US East -> 00:00 - 01:00) (EU -> 00:00 - 01:00)
 *     Plan #2: (US West -> 00:00 - 01:00) (US East -> 00:00 - 01:00) (EU -> 01:00 - 02:00)
 *     Plan #3: (US West -> 00:00 - 01:00) (US East -> 00:00 - 01:00) (EU -> 02:00 - 03:00)
 *               ....
 *     Plan  N: (US West -> 23:00 - 00:00) (US East -> 23:00 - 00:00) (EU -> 23:00 - 00:00)
 *
 * The cost function penalizes plans that are far from the desired start time, overlap with other deployments, or
 * overlap with a peak period. The idea is to schedule deployments only at times when it will minimize impact on
 * users as well as minimizing overlaps.
 *
 * It provides a best-effort attempt to schedule across all regions but will fail if it cannot fit all pushes across
 * all regions within 1 day of the start time. The reason is to limit computation. A production-class system might
 * expand that time horizon arbitrarily.
 *
 * Plans are created and streamed one at a time. Special care was taken to eliminate the forced evaluation of any
 * stream except during plan evaluation at which time it becomes necessary to examine each and every plan.
 *
 * In this implementation, every plan combination will be evaluated (exhaustive search) causing considerable
 * computation time should the number of regions increase or the default 30-minute slot be decreased. The result
 * is effectively O((n + m)!) where { n = the number of regions } and { m = the number of slots }.
 *
 * Cons
 * ---------
 *   1. Complexity-wise this approach will grow untenable very quickly. I propose utilizing a different technique like
 *      simulated annealing in order to bound computation and proceed. It would be a bad solution indeed were your
 *      scheduler to take longer to decide than the time it takes to do the actual deployment.
 *
 *   2. The tests are far from exhaustive.
 *
 *   3. Needs better comments and documentation.
 *
 *   4. Needs more refactoring to clean up the code and make it more understandable. For comprehensions within for
 *      comprehensions can make it a bit difficult to follow. Also the use of Joda-Time results in less-than-idiomatic
 *      Scala code and complicates understanding.
 *
 *   5. I would prefer to abstract everything into a TimeSpan instead of relying so heavily on Joda-Time's intervals
 *      to do the heavy lifting of determining overlap. Its use makes the code even more difficult to follow since
 *      it goes back-and-forth between these domains. At the very least one domain should have been chosen and
 *      adhered to more stringently.
 *
 *   6. Plan evaluation (and cost calculation) is very parallelizable yet this solution does not take advantage of
 *      the inherent parallelism.
 *
 *   7. Over-engineered for the problem.
 *
 *   8. Deployments are never removed from a schedule -- repeated calls to Schedule.deploy() yield a new Schedule
 *      instance with the previous agenda attached to the new one.
 *
 * Pros
 * ---------
 *   1. Once the required objects are instantiated and in place, the amount of consumed memory stabilizes as a large
 *      number of plans are evaluated.
 *
 *   2. From an outside perspective, the API is fully immutable (but the plan evaluation suffers from potential
 *      concurrency-related issues if parallelized).
 *
 *   3. API is referentially transparent. Schedule.deploy(x) = Schedule.deploy(y) if x = y and Schedule.deploy() is
 *      free of side effects and will always yield the same results for the same inputs.
 *
 *   4. The solution takes into account peak times or periods during which it should avoid scheduling a deployment.
 *
 *   5. Times are converted to UTC for comparisons and converted back into respective time zones only for display
 *      purposes. Plan evaluations are done in UTC.
 *
 *   6. Uses scala check to randomly generate regions and schedules and evaluate if they avoid peak times. Could be
 *      much more exhaustive though.
 */

import scala.math._
import org.joda.time._

object NaiveSchedulingStrategy extends SchedulingStrategy {
  def deploy(schedule: Schedule,
             buildID: BuildID,
             starting: ReadableDateTime,
             duration: ReadableDuration,
             driftThreshold: ReadableDuration,
             waitDelay: ReadableDuration,
             reporting: ReportingMagnet)
            : Traversable[AgendaItem] = {

    import Schedule._

    val current_agenda = schedule.agenda
    val regions = schedule.regions

    val drift_tolerance = driftThreshold.toDuration.getStandardSeconds

    val wait_delay_duration = waitDelay.toDuration
    val start = starting.toDateTime.withZone(TIME_ZONE_OF_RECORD)
    val deploy_duration = duration.toDuration
    val simulated_deploy_duration = deploy_duration.plus(wait_delay_duration)
    val max_distance_from_desired = DateTimeConstants.SECONDS_PER_DAY

    require(
      simulated_deploy_duration.getStandardMinutes > 0L,
      s"This scheduler requires a duration greater than 0 minutes"
    )

    require(
      (simulated_deploy_duration.getStandardSeconds * regions.length).toDouble / DateTimeConstants.SECONDS_PER_DAY <= 1.0D,
      s"This scheduler relies on a duration taking less than 1 day to complete across all regions"
    )

    type Plan = Seq[(TimeSpan, Interval, Region)]
    type Proposal = (ReadableDateTime, Region)

    val number_of_regions = regions.length
    val region_stream = regions.toStream

    case class CachedRegionData(maxPeakPenalty: Double, maxConcurrentPenalty: Double)

    //Pre-calculates useful information for each region.
    val region_data = (
      for(region <- regions)
        yield region -> {
          //Calculate peak duration.
          //Do-not-cache since the peak can move around depending on when a proposal wants to
          //run. This is still needed in calculating what the max penalty could be for running
          //during a peak period.
          val peak_duration = region
            .peak
            .toInterval(starting.toDateTime.withTimeAtStartOfDay(), region.timeZone)
            .toInterval
            .toDuration

          //Calculate what the maximum penalty could be.
          //It's the maximum amount of time we could take to deploy during a peak period in seconds.
          val duration =
            if (simulated_deploy_duration.isShorterThan(peak_duration))
              simulated_deploy_duration
            else
              peak_duration

          val max_peak_penalty = duration.getStandardSeconds


          //Calculate the maximum penalty for overlapping other deployments.
          //The max would be if we were to overlap them all.
          val max_concurrent_penalty = (
            for {
              AgendaItem(_, _, item_duration, item_region) <- current_agenda
              if item_region eq region
            } yield item_duration.toDuration.getStandardSeconds
          ).sum.toDouble

          CachedRegionData(max(1.0, max_peak_penalty), max(1.0, max_concurrent_penalty))
        }
      ).toMap

    //Do not recalculate this over and over again.
    //
    //Creates slots as Joda Intervals like:
    // <Starting> + 00:00 - 00:30
    // <Starting> + 00:30 - 01:00
    // <Starting> + 01:00 - 01:30
    // ...
    // <Starting> + 23:30 - 00:00
    def generateSlots() =
      SCHEDULE_SLOTS.map { slot =>
        //Calculate candidate interval in terms of starting time.
        (slot, slot
          .toIntervalAtTimeZone(start, TIME_ZONE_OF_RECORD)
          .toInterval
          .withDurationAfterStart(simulated_deploy_duration)
        )
      }

    //Generates potential plans by creating combinations of potential
    //time slots and eliminating overlaps across regions for the same
    //deployment (hard requirement).
    //
    //It then assigns a region to each time slot.
    //
    //Evaluation of each plan is deferred.
    def generatePlans(): Stream[Plan] =
      for {
        //Create a new combination of slots.
        combo <- generateSlots()
          .combinations(number_of_regions)
          .map(_.toList)
          .toStream

        //Eliminate overlaps across regions.
        if !anyOverlap(combo)

        //Assign a region to each slot.
        plan = for {
          ((candidate, candidate_interval), region) <- combo.zip(region_stream)
        } yield (candidate, candidate_interval, region)
      } yield plan

    //Determine if any proposals in the plan overlap.
    //If so, then these will need to be filtered out since we cannot allow it.
    def anyOverlap(combo: List[(TimeSpan, Interval)]): Boolean = {
      //See if any combination of slots would overlap with another.
      for {
        List((_, one), (_, two)) <- combo.combinations(2)
        if one.overlaps(two)
      } return true
      false
    }

    //Main logic that will affect the produced plan.
    def calculateCost(plan: Plan): Double = {
      var cost = 0.0
      for {
        (time, interval, region) <- plan

        //Calculate what the peak interval would be given this region's
        //candidate interval.
        peak_interval = region
          .peak
          .toInterval(interval.getStart.withTimeAtStartOfDay(), region.timeZone)
          .toInterval

        //Extract the max penalty that could be assigned from our cache of region data.
        CachedRegionData(max_peak_penalty, max_concurrent_penalty) = region_data(region)

        //Penalize schedules that overlap w/ peak hours.

        //Start by getting the overlap between this duration and the peak and
        //how long we would be in the peak period in seconds.
        //
        //Use Option(...) since .overlap() can return null.
        overlap_with_peak = Option {
          interval.overlap(peak_interval)
        }

        //Calculate how much we overlap with the peak period.
        //The more we overlap the costlier it needs to be.
        overlap_with_peak_penalty = overlap_with_peak
          .getOrElse(new Interval(0L, 0L))
          .toDuration
          .getStandardSeconds

        //Calculate the penalty for operating during peak hours.
        peak_overlap_penalty = overlap_with_peak_penalty.toDouble / max_peak_penalty.toDouble

        //We want to stay as close to our desired starting time as possible.
        //So an additional cost is how far away from the start are we.

        (distance_start, distance_end) =
          if (start.isBefore(interval.getStart))
            (start, interval.getStart)
          else
            (interval.getStart, start)

        //How far away from our desired time are we?
        distance_from_desired_time = new Interval(distance_start, distance_end)
          .toDuration
          .getStandardSeconds

        distance_from_desired_time_penalty = distance_from_desired_time / max_distance_from_desired

        //How close are we to other scheduled deployments? Try to avoid overlapping.

        total_concurrent_overlap = (
          for {
            AgendaItem(_, item_at, item_duration, item_region) <- current_agenda
            if item_region eq region

            item_at_tz_of_record = item_at
              .toDateTime
              .withZone(TIME_ZONE_OF_RECORD)

            item_interval = new Interval(
              item_at_tz_of_record,
              item_at_tz_of_record.plus(item_duration)
            )

            //Find out how much overlap there is b/t 2 deployments.
            item_overlap = Option {
              item_interval.overlap(interval)
            }

            if item_overlap.isDefined

            item_overlap_seconds = item_overlap.get.toDuration.getStandardSeconds

          } yield {
            math.max(0, item_overlap_seconds - drift_tolerance)
          }
        ).sum.toDouble

        concurrent_penalty = total_concurrent_overlap / max_concurrent_penalty
      } {
        //It's more costly to deploy during peak times than not starting right away.
        //Avoid overlapping as much as possible.
        cost += 20 * distance_from_desired_time_penalty + 50 * concurrent_penalty + 100 * peak_overlap_penalty
      }
      cost
    }

    //Could parallelize plan evaluation?
    //Not strictly needed but may speed things up if there are lots of regions + slots.
    //Could slow things down if there's a small amount.

    var lowest_cost_so_far = Double.PositiveInfinity
    var best_plan_found: Option[Plan] = None

    import scala.util.control.Breaks._

    breakable {
      for {
        plan <- generatePlans()      //Streams new plans on-demand. Carefully crafted so this works.
        cost = calculateCost(plan)   //Get a score that represents how costly one plan is vs. another.
                                     //If we find a plan that's better than another, then use that.
        if cost < lowest_cost_so_far || lowest_cost_so_far == Double.PositiveInfinity
      } {
        lowest_cost_so_far = cost
        best_plan_found = Some(plan)

        //println(s"$cost $plan")

        //Stop processing if we come across a plan that will work.
        if (cost <= 0.0)
          break()
      }
    }

    //If there is no plan, then we have a problem.
    if (best_plan_found.isEmpty)
      throw new IllegalStateException(s"Unable to create a deployment schedule")

    val best =
      best_plan_found.get

    //Transform a plan into an AgendaItem.
    val best_with_time_zone: Seq[AgendaItem] =
      best.map { case (time: TimeSpan, interval: Interval, region: Region) =>

        val new_agenda_item =
          AgendaItem(buildID, interval.getStart.withZone(region.timeZone), deploy_duration, region)

        //Look for drift (will this overlap w/ an existing deployment?)
        for {
          item @ AgendaItem(_, item_at, item_duration, item_region) <- current_agenda

          //Only take a look if we're not in the same region.
          if item_region ne region

          item_at_tz_of_record = item_at
            .toDateTime
            .withZone(TIME_ZONE_OF_RECORD)

          item_interval = new Interval(
            item_at_tz_of_record,
            item_at_tz_of_record.plus(item_duration)
          )

          //Find out how much overlap there is b/t 2 deployments.
          item_overlap = Option {
            item_interval.overlap(interval)
          }

          //Indicates if there's actually an overlap or not.
          if item_overlap.isDefined

          item_overlap_interval = item_overlap.get

          //Translate to region's time zone.
          //item_overlap_interval = new Interval(
          //  item_overlap_interval_tz_of_record.getStart.withZone(item_region.timeZone),
          //  item_overlap_interval_tz_of_record.getEnd.withZone(item_region.timeZone)
          //)

          if item_overlap_interval.toDuration.isLongerThan(driftThreshold)

        } reporting.drift(item, new_agenda_item, item_overlap_interval)

        new_agenda_item
      }

    //All done!
    current_agenda.toSeq ++ best_with_time_zone
  }
}
