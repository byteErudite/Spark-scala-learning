import org.apache.spark.sql.SparkSession

object CreatingSparkContextWithSparkSession {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("First spark application")
      .master("local")
      .getOrCreate()

    val array = Array(10,20,30,40,50)
    val arrRdd = sparkSession.sparkContext.parallelize(array, 2)
    arrRdd.foreach(println)
  }
}
