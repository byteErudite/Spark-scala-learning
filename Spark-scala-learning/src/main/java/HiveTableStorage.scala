import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object HiveTableStorage {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("First spark application")
      .master("local")
      .getOrCreate()

    val hiveContext = new HiveContext(sparkSession.sparkContext)
    import hiveContext.implicits._

    val arrRdd = sparkSession.sparkContext.parallelize(Array(("Vaibhav","20"), ("Amit","21")))
    val nameDF = arrRdd
      .map{v=>  (v._1,v._2)}
      .toDF("name", "age")

    nameDF.registerTempTable("name_table")
    nameDF.write.saveAsTable("name_table")

    hiveContext.sql("select * from name_table")

    val schema = StructType(
      StructField("Integer as String", StringType, false)::Nil
    )
    val rowRdd = arrRdd.map(l => Row(l))

    val df = sparkSession.createDataFrame(rowRdd, schema)
    df.printSchema()
    df.show()

  }
}
