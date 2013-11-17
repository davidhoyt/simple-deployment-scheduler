
import java.util.UUID
import org.joda.time.format.DateTimeFormat
import scala.collection._
import org.joda.time._

import scala.language.implicitConversions

sealed case class TimeSpan(starting: LocalTime, ending: LocalTime) {
  def toInterval(date: ReadableDateTime, timeZone: DateTimeZone): ReadableInterval = {
    val dt = date.toDateTime
    val d = dt.withZoneRetainFields(timeZone).withZone(Schedule.TIME_ZONE_OF_RECORD)
    //Is this still on the same day?
    val next =
      if (d.toLocalDate.isBefore(dt.toLocalDate))
        d.plusDays(1)
      else
        d
    val start = next.plusMillis(starting.millisOfDay().get())
    val end = next.plusMillis(ending.millisOfDay().get())
    val ends =
      if (end.isBefore(start))
        end.plusDays(1)
      else
        end

    new Interval(start, ends)
  }

  def toIntervalAtTimeZone(date: ReadableDateTime, timeZone: DateTimeZone): ReadableInterval = {
    val d = date.toDateTime.withZoneRetainFields(timeZone)
    val start = d.plusMillis(starting.millisOfDay().get())
    val end = d.plusMillis(ending.millisOfDay().get())
    val ends =
      if (end.isBefore(start))
        end.plusDays(1)
      else
        end
    new Interval(start, ends)
  }

  def isEmpty =
    ending.isEqual(starting)
}

sealed case class Region(name: String, timeZone: DateTimeZone, peak: TimeSpan)

sealed case class BuildID(id: UUID = UUID.randomUUID())
object BuildID {
  def apply(id: String): BuildID =
    BuildID(UUID.fromString(id))
}

sealed case class AgendaItem(buildID: BuildID, at: ReadableDateTime, duration: ReadableDuration, region: Region) {
  override def toString = {
    val date = at.toDateTime
    val peak = region
      .peak
      .toIntervalAtTimeZone(date.withTimeAtStartOfDay(), region.timeZone)

    s"AgendaItem(${buildID.id}, ${Schedule.DATE_TIME_FORMAT.print(date)}, $duration, ${region.name} @ ${Schedule.interval2String(peak)})"
  }
}

trait ReportingMagnet {
  def drift(item1: AgendaItem,
            item2: AgendaItem,
            interval: ReadableInterval)
           : Unit
}

trait SchedulingStrategy {
  def deploy(schedule: Schedule,
             buildID: BuildID,
             starting: ReadableDateTime,
             duration: ReadableDuration,
             driftThreshold: ReadableDuration,
             waitDelay: ReadableDuration,
             reporting: ReportingMagnet)
            : Traversable[AgendaItem]
}

sealed case class Schedule(agenda: Traversable[AgendaItem], regions: Seq[Region], reporting: ReportingMagnet, strategy: SchedulingStrategy) {
  def deploy(starting: ReadableDateTime,
             duration: Long = 1L,
             buildID: BuildID = BuildID(),
             driftThreshold: Int = Schedule.DEFAULT_DRIFT_THRESHOLD,
             waitDelay: Long = 0L)
            : Schedule = {
    newDeployment(
      buildID,
      starting,
      Duration.standardHours(duration),
      Duration.standardMinutes(driftThreshold.toLong),
      Duration.standardHours(waitDelay)
    )
  }

  def newDeployment(buildID: BuildID,
                    starting: ReadableDateTime,
                    duration: ReadableDuration,
                    driftThreshold: ReadableDuration,
                    waitDelay: ReadableDuration)
                   : Schedule = {
    new Schedule(
      strategy.deploy(
        this,
        buildID,
        starting,
        duration,
        driftThreshold,
        waitDelay,
        reporting
      ),
      regions,
      reporting,
      strategy
    )
  }
}

object Schedule {
  //Difference (in minutes) over which to attempt to fit a schedule.
  val SCHEDULE_ATOM = 30

  //Length of time (in minutes) over which to not report drift.
  val DEFAULT_DRIFT_THRESHOLD = 10

  val US_WEST_TIME_ZONE    = DateTimeZone.forID("America/Los_Angeles")
  val US_EAST_TIME_ZONE    = DateTimeZone.forID("America/New_York")
  val EU_IRELAND_TIME_ZONE = DateTimeZone.forID("Europe/Dublin")
  val TIME_ZONE_OF_RECORD  = DateTimeZone.UTC

  val DATE_TIME_FORMAT = DateTimeFormat.forPattern("MM-dd-yyyy hh:mm:ss a z")
  val LOCAL_TIME_CONSTANT = new LocalTime(0L, TIME_ZONE_OF_RECORD)

  def interval2String(i: ReadableInterval): String =
    s"(${DATE_TIME_FORMAT.print(i.getStart)} - ${DATE_TIME_FORMAT.print(i.getEnd)})"

  implicit val DEFAULT_REGIONS = Seq(
    Region("US-West (Oregon)",   US_WEST_TIME_ZONE,    TimeSpan(new LocalTime(0, 1), new LocalTime(15, 0)) /*PST*/),
    Region("US-East (Virginia)", US_EAST_TIME_ZONE,    TimeSpan(new LocalTime(0, 1), new LocalTime(15, 0)) /*EST*/),
    Region("EU (Ireland)",       EU_IRELAND_TIME_ZONE, TimeSpan(new LocalTime(0, 1), new LocalTime(15, 0)) /*UTC*/)
  )

  lazy val SCHEDULE_SLOTS = Stream.from(SCHEDULE_ATOM, SCHEDULE_ATOM).takeWhile(_ <= DateTimeConstants.MINUTES_PER_DAY).map { ending: Int =>
    val starting = ending - SCHEDULE_ATOM
    TimeSpan(LOCAL_TIME_CONSTANT.plusMinutes(starting), LOCAL_TIME_CONSTANT.plusMinutes(ending))
  }

  implicit val DEFAULT_SCHEDULING_STRATEGY =
    NaiveSchedulingStrategy

  implicit val DEFAULT_REPORTING_NOOP = new ReportingMagnet {
    def drift(item1: AgendaItem, item2: AgendaItem, interval: ReadableInterval): Unit = {}
  }

  val DEFAULT_REPORT_DRIFT_NOOP: ReportDrift = {case(_, _, _) => {}}

  type ReportDrift = PartialFunction[(AgendaItem, AgendaItem, ReadableInterval), Unit]

  implicit def partialDriftCallback2ReportingMagnet(fnDrift: PartialFunction[(AgendaItem, AgendaItem, ReadableInterval), Unit]): ReportingMagnet =
    new ReportingMagnet {
      def drift(item1: AgendaItem, item2: AgendaItem, interval: ReadableInterval): Unit = {
        val parameters = (item1, item2, interval)
        if (fnDrift.isDefinedAt(parameters))
          fnDrift(parameters)
      }
    }

  def apply(starting: ReadableDateTime, duration: Long = 1L, buildID: BuildID = BuildID(), driftThreshold: Int = DEFAULT_DRIFT_THRESHOLD, waitDelay: Long = 0L)
           (fnDrift: ReportDrift)
           (implicit regions: Seq[Region], strategy: SchedulingStrategy)
           : Schedule = {
    newDeployment(
      buildID,
      starting,
      Duration.standardHours(duration),
      Duration.standardMinutes(driftThreshold),
      Duration.standardHours(waitDelay)
    )(regions, partialDriftCallback2ReportingMagnet(fnDrift), strategy)
  }

  def newDeployment(buildID: BuildID, starting: ReadableDateTime, duration: ReadableDuration, driftThreshold: ReadableDuration, waitDelay: ReadableDuration)
                   (implicit regions: Seq[Region], reporting: ReportingMagnet, strategy: SchedulingStrategy)
                   : Schedule = {
    new Schedule(Seq(), regions, reporting, strategy)
      .newDeployment(
        buildID,
        starting,
        duration,
        driftThreshold,
        waitDelay
      )
  }
}
