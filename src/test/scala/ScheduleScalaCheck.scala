
import org.joda.time._

import org.scalacheck._
import Arbitrary._
import Gen._

object ScheduleScalaCheck {
  import scala.collection.JavaConversions._
  import Schedule._

  val MAX_REGION_SIZE = 3

  val TIME_ZONE_IDS = DateTimeZone.getAvailableIDs.toIndexedSeq
  val TIME_ZONE_ID_SIZE = TIME_ZONE_IDS.size

  lazy val GenHours1to3: Gen[Hours] =
    for(hour <- choose(1, 3))
      yield Hours.hours(hour)

  lazy val GenTimeZone: Gen[DateTimeZone] =
    for(idx <- choose(0, TIME_ZONE_ID_SIZE - 1))
      yield DateTimeZone.forID(TIME_ZONE_IDS(idx))

  lazy val GenTimeSpan0to6: Gen[TimeSpan] =
    for(start <- choose(0, 60 * 3); end <- choose(60 * 3, 60 * 6))
      yield {
        val (s, e) =
          if (start <= end)
            (start, end)
          else
            (end, start)
        TimeSpan(LOCAL_TIME_CONSTANT.plusMinutes(s), LOCAL_TIME_CONSTANT.plusMinutes(e))
      }

  object PeakGenerators {
    implicit lazy val PeakHour = Arbitrary(GenHours1to3)
    implicit lazy val PeakTimeSpan = Arbitrary(GenTimeSpan0to6)
    implicit lazy val PeakRegion = Arbitrary(GenPeakRegion)
    implicit lazy val PeakSeqRegions = Arbitrary(GenPeakSeqRegions)
    implicit lazy val PeakTimeZone = Arbitrary(GenTimeZone)

    lazy val GenPeakRegion: Gen[Region] =
      for {
        random_name <- alphaStr
        random_peak <- arbitrary[TimeSpan]
        random_time_zone <- arbitrary[DateTimeZone]
      } yield Region(random_name, random_time_zone, random_peak)

    lazy val GenPeakSeqRegions: Gen[Seq[Region]] =
      resize(MAX_REGION_SIZE, listOf(GenPeakRegion))
  }
}