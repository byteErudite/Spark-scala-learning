import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameWithSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("First spark application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val arrRdd = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9))

    val schema = StructType(
      StructField("Number", IntegerType, false)::Nil
    )

    val rowRdd = arrRdd.map(l => Row(l))
    val dataFrame = sqlContext.createDataFrame(rowRdd, schema)
    dataFrame.printSchema()
    dataFrame.show()
  }
}
