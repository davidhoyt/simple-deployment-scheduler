
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.junit.JUnitRunner
import org.scalatest.{SeveredStackTraces, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.matchers.ShouldMatchers
import org.joda.time._

import org.scalatest.prop.Checkers
import org.scalacheck._
import Prop._

@RunWith(classOf[JUnitRunner])
class ScheduleSuite extends FunSuite
                     with ShouldMatchers
                     with SeveredStackTraces
                     with Checkers {

  import ScheduleScalaCheck._
  import ScheduleScalaTest._
  import Schedule._

  val STRATEGY = implicitly[SchedulingStrategy]

  object PeakProperties extends Properties("Peak") {
    import PeakGenerators._

    property("Nothing is scheduled during peak times") =
      forAll { (tz: DateTimeZone, regions: Seq[Region], duration: Hours) =>
        validateNothingScheduledDuringPeakHours(
          Schedule(todayAt(12, tz), duration.getHours)(DEFAULT_REPORT_DRIFT_NOOP)(regions, STRATEGY)
        )
      }
  }

  test("Works for just 1 region") {
    val regions = Seq(
      Region("R1", tz("Europe/Oslo"), TimeSpan(new LocalTime(0, 10), new LocalTime(5, 29)))
    )
    val schedule = Schedule(todayAt(12, tz("Asia/Taipei")), 3)(DEFAULT_REPORT_DRIFT_NOOP)(regions, STRATEGY)

    validateNothingScheduledDuringPeakHours(schedule) should be (true)
  }

  test("Works when time zone changes move peak back a day") {
    val regions = Seq(
      Region("R1", tz("Antarctica/McMurdo"),         TimeSpan(new LocalTime( 3, 21), new LocalTime( 7,  7))),
      Region("R2", tz("America/Argentina/La_Rioja"), TimeSpan(new LocalTime( 1, 16), new LocalTime( 4, 24))),
      Region("R3", tz("MET"),                        TimeSpan(new LocalTime( 1,  4), new LocalTime( 5,  9)))
    )
    val schedule = Schedule(todayAt(12, tz("America/Sao_Paulo")), 2)(DEFAULT_REPORT_DRIFT_NOOP)(regions, STRATEGY)

    validateNothingScheduledDuringPeakHours(schedule) should be (true)
  }

  test("Drift is reported properly") {
    val drift_count = new AtomicInteger(0)

    //Default regions and strategy are picked up and used.
    val schedule = Schedule(todayAt(16, US_WEST_TIME_ZONE), 1L) { case (item1, item2, interval) =>
      //println(s"DRIFT: (${item1.buildID.id} ${item1.region.name}) vs (${item2.buildID.id} ${item2.region.name}) -- ${interval2String(interval)}")

      drift_count.incrementAndGet()
    }

    //Should carry the drift reporting automagically.

    //Make 3 deployments.
    schedule
      .deploy(todayAt(17, US_WEST_TIME_ZONE), 1L)
      .deploy(todayAt(18, US_WEST_TIME_ZONE), 1L)
      .deploy(todayAt(19, US_WEST_TIME_ZONE), 1L)

    //Count really has nothing to do w/ the # of deployments.
    //This just happens to coincide.
    drift_count.get() should be (3)
  }

  test("Schedule meets criterion for peak scheduling") {
    check(PeakProperties)
  }

  //Test behavior over DST
}