import org.apache.spark.sql.SparkSession

case class Superhero(name: String, superpower: String, age: Integer)
object CreatingDataset {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("First spark application")
      .master("local")
      .getOrCreate()



    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val arrRdd = sparkSession.sparkContext.parallelize(array, 2)
    case class SEO(seo_url_en:Option[String], seo_url_fr:Option[String], expiry_date:Option[Long] = None, last_modified:Option[Long] = None)
    val dataRdd = sparkSession.sparkContext.parallelize(Array(("","",12548,12548), ("","",12548,12548)))
    dataRdd
      .map(value => (value._1,value._2,value._3))
      .asInstanceOf[SEO]




    println("number of elements in rdd: " + arrRdd.count())
    arrRdd.foreach(println)


    import sparkSession.implicits._

    val heroDS = sparkSession.read
//      .option("header","false")
//      .option("inferSchema", "true")
      .textFile("C:/Users/user/Desktop/Repositories/JavaRepos/Spark-scala-Learning-projects/Spark-scala-learning/src/main/resources/Data.txt")
      .map(line => line.split("    "))
      .map{case Array(f,g,h) => Superhero(f,g,h.toInt)}
//      .as[Superhero]

    heroDS.show()
  }
}
