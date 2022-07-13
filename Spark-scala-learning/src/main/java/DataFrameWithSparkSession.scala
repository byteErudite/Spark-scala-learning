import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameWithSparkSession {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("First spark application")
      .master("local")
      .getOrCreate()

    val arrRdd = sparkSession.sparkContext.parallelize(Array("10","20","30","40","50"))
    val schema = StructType(
      StructField("Integer as String", StringType, false)::Nil
    )
    val rowRdd = arrRdd.map(l => Row(l))

    val df = sparkSession.createDataFrame(rowRdd, schema)
    df.printSchema()
    df.show()

  }
}
