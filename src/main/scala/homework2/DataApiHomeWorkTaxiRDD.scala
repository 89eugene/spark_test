package homework2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, count}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.immutable
object DataApiHomeWorkTaxiRDD {
  /**
   * Метод выполняет чтение файла в формате паркет
   *
   * @param path  - путь к файлу
   * @param spark - spark session
   * @return
   */
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  /**
   * Метод выполняет выборку
   *
   * @return
   */
  def processTaxiData(values: DataFrame, spark: SparkSession) = {
    import spark.implicits._

    val taxiFactsDS: Dataset[TaxiRide] = values.as[TaxiRide]

    val taxiFactsRDD: RDD[TaxiRide] = taxiFactsDS.rdd

    val makeResult: immutable.Seq[Result] = taxiFactsRDD
      .map(c => (c.tpep_pickup_datetime, c))
      .countByKey()
      .toList.sortBy(-_._2)
      .map(r => Result(r._1, r._2))

    val result = makeResult.toDF()
    result.show()
    result
  }

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val values: DataFrame = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    values.cache()

    val processResult = processTaxiData(values, spark)

    processResult.rdd.coalesce(1, shuffle = false)
      .map(_.toSeq.map(_ + " ").reduce(_ + " " + _))
      .saveAsTextFile("timeRange/result")

  }

    case class TaxiRide(
                         VendorID: Int,
                         tpep_pickup_datetime: String,
                         tpep_dropoff_datetime: String,
                         passenger_count: Int,
                         trip_distance: Double,
                         RatecodeID: Int,
                         store_and_fwd_flag: String,
                         PULocationID: Int,
                         DOLocationID: Int,
                         payment_type: Int,
                         fare_amount: Double,
                         extra: Double,
                         mta_tax: Double,
                         tip_amount: Double,
                         tolls_amount: Double,
                         improvement_surcharge: Double,
                         total_amount: Double
                       )

    case class Result(
                       time: String,
                       count: Long
                     )

}

