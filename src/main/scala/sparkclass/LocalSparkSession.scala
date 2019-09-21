package sparkclass

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocalSparkSession {
  val conf: SparkConf =
    new SparkConf()
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")

  val spark: SparkSession =
    SparkSession.builder
      .config(conf)
      .master("local")
      .appName("spark-class")
      .getOrCreate()
}
