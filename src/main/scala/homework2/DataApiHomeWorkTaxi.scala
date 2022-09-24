package homework2

import org.apache.spark.sql.functions.{broadcast, col, count, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataApiHomeWorkTaxi{

  /**
   * Метод выполняет чтение файла в формате паркет
   * @param path - путь к файлу
   * @param spark - spark session
   * @return
   */
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  /**
   * Метод выполняет чтение файла в формате CSV
   * @param path - путь к файлу
   * @param spark - spark session
   * @return
   */
  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  /**
   * Метод выполняет записать Dataset результата в паркет файл
   * @param result - Dateset result
   * @param path - путь к файлу
   * @param spark - spark session
   */
  def writeParquet(result: Dataset[Row], path: String)(implicit spark: SparkSession) =
    result.toDF().write.parquet(path)

  /**
   * Метод выполняет выборку
   * @return
   */
  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame) = {
    taxiFactsDF.join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total_trips")
      )
      .orderBy(col("total_trips").desc)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiFactsDF: DataFrame = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    taxiFactsDF.cache()
    val taxiZoneDF: DataFrame = readCSV("src/main/resources/data/taxi_zones.csv")
    taxiZoneDF.cache()

    val result: Dataset[Row] = processTaxiData(taxiFactsDF, taxiZoneDF)

    result.show()

    writeParquet(result, "./borough")
  }
}

