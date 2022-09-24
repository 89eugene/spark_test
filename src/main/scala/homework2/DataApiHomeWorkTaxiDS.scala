package homework2

import org.apache.spark.sql.functions.{col, count, max, mean, min, round, stddev_pop}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object DataApiHomeWorkTaxiDS {
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
  def processTaxiData(taxiValues: Dataset[TaxiRide]) = {
    taxiValues.agg(
      count("*").as("total_trips"),
      round(min("trip_distance"), 2).as("min_distance"),
      round(max("trip_distance"), 2).as("max_distance"),
      round(mean("trip_distance"), 2).as("mean_distance"),
      round(stddev_pop("trip_distance"), 2).as("standard_deviation")
    )
      .orderBy(col("total_trips").desc)
  }

  def main(args: Array[String]): Unit = {


    implicit val spark = SparkSession.builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5433/otus"
    val user = "docker"
    val password = "docker"

    import spark.implicits._

    val taxiValues: Dataset[TaxiRide] = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018").as[TaxiRide]

    val result = processTaxiData(taxiValues)
    result.show()

    result.select("total_trips", "min_distance", "max_distance", "mean_distance", "standard_deviation")
      .write.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", "TaxiResult")
      .option("user", user)
      .option("password", password)
      .save()
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
}

