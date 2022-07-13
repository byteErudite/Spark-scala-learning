import org.apache.spark.{SparkConf, SparkContext}

object CreatingSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkConf()
    sparkContext.setAppName("First spark application")
    sparkContext.setMaster("local")

    val sc = new SparkContext(sparkContext)
    val array = Array(1,2,3,4,5,6,7,8,9)
    val arrRdd = sc.parallelize(array, 2)

    println("number of elements in rdd: "+arrRdd.count())
    arrRdd.foreach(println)

    val fileRdd = sc.textFile("C:/Users/user/Desktop/Repositories/JavaRepos/Spark-scala-Learning-projects/Spark-scala-learning/src/main/resources/Data.txt", 5)
    fileRdd.flatMap(line => line.split(" ")).foreach(println)
  }
}
