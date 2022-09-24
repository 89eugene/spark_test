package homework2

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import homework2.DataApiHomeWorkTaxiDS.{TaxiRide, processTaxiData, readParquet}


class DataApiHomeWorkTaxiDSTest extends SharedSparkSession {
  import testImplicits._

  test("test for processTaxiData") {
    val taxiDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018").as[TaxiRide]

    val actualDistribution = processTaxiData(taxiDF)

    checkAnswer(
      actualDistribution,
      Row(331893, 0.0, 66.0, 2.72, 3.49) ::Nil
    )
  }
}

