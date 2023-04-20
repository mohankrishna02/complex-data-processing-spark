package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source
object Project {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Phase2Project")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ss = SparkSession.builder().getOrCreate()
    //reading projectsample.avro file
    val avrodf = ss.read.format("avro").load("D:/BigData/Sparkfolder/workspace/Phase2Project/Datasets/projectsample.avro")
    avrodf.show()
    //reading data from url
    val rand = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
    val urldata = rand.mkString
    //url data converting into a rdd
    val rdd = sc.parallelize(List(urldata))
    //rdd converting into dataframe
    val df = ss.read.json(rdd)
    df.show()
    df.printSchema()
    val flatdf = df.withColumn("results", expr("explode(results)"))
    flatdf.printSchema()
    //flattening the complex json data
    val finaldf = flatdf.select(
      "nationality",
      "seed",
      "version",
      "results.user.cell",
      "results.user.dob",
      "results.user.email",
      "results.user.gender",
      "results.user.location.city",
      "results.user.location.state",
      "results.user.location.street",
      "results.user.location.zip",
      "results.user.md5",
      "results.user.name.first",
      "results.user.name.last",
      "results.user.name.title",
      "results.user.password",
      "results.user.phone",
      "results.user.picture.large",
      "results.user.picture.medium",
      "results.user.picture.thumbnail",
      "results.user.registered",
      "results.user.salt",
      "results.user.sha1",
      "results.user.sha256",
      "results.user.username")

    finaldf.show()
    //removing the numericals from username column
    val procdf = finaldf.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
    procdf.show()
    //performing the left join
    val joindf = avrodf.join(broadcast(procdf), Seq("username"), "left")
    joindf.show()
    //filtering the nationlity is null
    val nulldf = joindf.filter(col("nationality").isNull)
    nulldf.show()
    //filtering the nationlity is not null
    val notnulldf = joindf.filter(col("nationality").isNotNull)
    notnulldf.show()
    //adding the date column to not avaiable and available customers
    val finalnulldf = nulldf.withColumn("date", current_date()).show()
    val finalnotnulldf = notnulldf.withColumn("date", current_date()).show()

  }
}