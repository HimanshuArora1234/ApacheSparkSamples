import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


/**
 * Created by arorah on 22/08/2016.
 */
object Sample1 {
  def main(args: Array[String]) {
    val inputFile = "test.txt"

    //Create spark conf, master as local with only one thread
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //Converting input text file into RDD (with 2 partition of data stream)
    //And caching the data coz we are going to access this too many times
    val inputData = sc.textFile(inputFile, 2).cache()

    //count lines containing a & b
    val numAs = inputData.filter(line => line.contains("a")).count()
    val numBs = inputData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    //Word count using famous Map reduce of data world
    var wordCounts = inputData.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCounts.collect().foreach(println)

  }
}
