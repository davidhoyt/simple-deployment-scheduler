
import org.joda.time._

object ScheduleScalaTest {
  import Schedule._

  val START_OF_TODAY = DateTime.now().withTimeAtStartOfDay()

  def today(tz: DateTimeZone) =
    START_OF_TODAY.withZoneRetainFields(tz)

  def todayAt(hour: Int, tz: DateTimeZone) =
    today(tz).withHourOfDay(hour)

  def tz(id: String) =
    DateTimeZone.forID(id)

  def validateNothingScheduledDuringPeakHours(schedule: Schedule): Boolean =
    schedule.agenda forall { item =>
      val peak_interval = item.region.peak
        .toInterval(item.at.toDateTime.withTimeAtStartOfDay(), item.region.timeZone)
        .toInterval

      val item_at_tz_of_record = item.at
        .toDateTime
        .withZone(TIME_ZONE_OF_RECORD)

      val item_interval = new Interval(
        item_at_tz_of_record,
        item_at_tz_of_record.plus(item.duration)
      )

      //Find out how much overlap there is b/t the peak and the item.
      val item_overlap = Option {
        item_interval.overlap(peak_interval)
      }

      //Indicates if there's actually an overlap or not.
      item_overlap.isEmpty
    }
}