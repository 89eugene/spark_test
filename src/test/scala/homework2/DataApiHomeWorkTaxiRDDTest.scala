package homework2

import homework2.DataApiHomeWorkTaxiRDD.{processTaxiData, readParquet}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class DataApiHomeWorkTaxiRDDTest extends AnyFlatSpec {


  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test for Data Api Rdd")
    .getOrCreate()

  it should "test for process taxidata" in {
    val taxiDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiData(taxiDF, spark)
      .collectAsList()

    assert(actualDistribution.get(0).get(0) == "2018-01-25 09:51:56")
    assert(actualDistribution.get(0).get(1) == 18)

    assert(actualDistribution.get(19).get(0) == "2018-01-25 22:40:30")
    assert(actualDistribution.get(19).get(1) == 15)
  }

}
