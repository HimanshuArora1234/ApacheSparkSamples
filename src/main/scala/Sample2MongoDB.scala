import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}


import com.mongodb.hadoop.{
MongoInputFormat, MongoOutputFormat,
BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.bson.BSONObject

/**
 * RDD manipulations backed by MongoDB collection.
 * Created by arorah on 22/08/2016.
 */
object Sample2MongoDB extends App {
  val mongoConfig = new Configuration()
  mongoConfig.set("mongo.input.uri", "mongodb://localhost:27017/sparkDb.User") //Connecting with a local MongoDB database `sparkDb`, collection requested `User`

  /**
   * Format of data in User collection
   * { "_id" : ObjectId("57bb083a5ce6b1e2771f5e1c"),
   *  "name" : "Himanshu",
   *  "family_name" : "Arora",
   *  "age" : 26,
   *  "email" : "x.y@c.c",
   *  "tel" : "0123456789",
   *  "address" : "Run jean jaures, 75001 Paris, France" }
   */

  val conf = new SparkConf().setAppName("Simple2 with Mongo DB").setMaster("local")
  val sc = new SparkContext(conf)

  // Create an RDD backed by the MongoDB collection.
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject]).cache() // Value type

  documents.values.filter(doc => doc.get("age").asInstanceOf[Double] > 25.0).collect().foreach(println)
  val avgAge = documents.values.map(_.get("age").asInstanceOf[Double]).mean()
  println("Average age = " + avgAge)
}
