import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreatingSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("First spark application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    val array = Array(1,2,3,4,5,6,7,8,9)
    val arrRdd = sc.parallelize(array, 2)

    println("number of elements in rdd: "+arrRdd.count())
    arrRdd.foreach(println)

    val fileRdd = sc.textFile("C:/Users/user/Desktop/Repositories/JavaRepos/Spark-scala-Learning-projects/Spark-scala-learning/src/main/resources/Data.txt", 5)
    fileRdd.foreach(line => {
      val lineValues = line.split("    ")
      val name = lineValues(0)
      val power = lineValues(1)
      val age = lineValues(2)
      println("Name : "+name+"\nPower: "+power+"\nage : "+age)
    })

    val heroNames = fileRdd.map(line => line.split("    ")(0).trim.toUpperCase())
    heroNames.foreach(println)
    heroNames.saveAsTextFile("output_files")

    //fileRdd.flatMap(line => line.split(" ")).foreach(println)
  }
}
