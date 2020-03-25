import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}





import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._





object ScalaPlusHive extends  App {


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .config("hive.metastore.uris", "thrift://192.168.1.50.cloudera:9083") // replace with your hivemetastore service's thrift url
    //thrift://quickstart.cloudera:9083
    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  //val df2 = spark

  val df2 = spark.table("landing.e7_th_precios")

  df2.show()

  val df = spark.read.textFile("in/airports.text")

  df.show()


  /*val conf = new SparkConf().setAppName("app")
    //.setMaster("spark://192.168.1.50:7077")
    .setMaster("local")
  val sc = new SparkContext(conf)
  //val sqlContext = new HiveContext(sc)


  //val test_enc_orc = hiveContext.sql("select * from test_enc_orc")


  val distFile = sc.textFile("in/airports.text")

  val words = distFile.flatMap(line => line.split(","))

  val wordsC = words.map(words => (words, 1))

  val wordsR = wordsC.reduceByKey((x,y) => x + y)
    .map((x) => (x._2, x._1))
    .sortByKey(ascending=false)
    .map((x) => (x._2, x._1))
    .take(20)


  for (w <- wordsR)
    println(w)

    */

  /*val output = words.reduce((x,y) => x +y)

   for (w <- output)
     println(w)*/


  // val wordCounts = words.countByValue()
  //for ((word, count) <- wordCounts) println(word + " : " + count)
  //val distFile = sc.textFile("in/airports.txt")



  /*Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)*/

  /*val spark = SparkSession
      .builder()
      .appName("Cloudera Sample Job")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    */

  // .master("yarn")
  //.config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.50:8020")
  //.config("spark.hadoop.yarn.resourcemanager.address", "192.168.1.50:8032")
  //.getOrCreate()

  println("=========== Spark Sample Job in Yarn Node =============")

}