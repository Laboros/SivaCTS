package example

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset




object GeoIsTransformation extends Greeting with App {
  
  val conf = new SparkConf().setAppName("test").setMaster("local")
  
  val spark: SparkSession = SparkSession.builder
 .config(conf)
 .getOrCreate()
 
 val dataframe_mysql = spark.sqlContext.read.format("jdbc").
 option("url", "jdbc:mysql://localhost/test").
 option("driver", "com.mysql.jdbc.Driver").
 option("dbtable", "test.person").option("user", "root").
 option("password", "password").load()
 
 
 import spark.implicits._
 println(dataframe_mysql.sqlContext.tableNames())
 var ds = dataframe_mysql.as[Person] 
  
 
  
   /*Seq(Person("Andy",32)).toDF().write.mode("append").format("jdbc").
   option("url", "jdbc:mysql://localhost/test").
 option("driver", "com.mysql.jdbc.Driver").
 option("dbtable", "test.person").option("user", "root").
 option("password", "password").save()*/
  
// Encoders are created for case classes
 /*val caseClassDS = Seq(Person("Andy", 32)).toDS()
  caseClassDS.show()
 
   var sourceDf = spark.table("exp_geo_is_cg").as(Person)
   */
 }

