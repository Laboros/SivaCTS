package example

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row




object GeoIsTransformation extends App {
  
  val conf = new SparkConf().setAppName("test").setMaster("local")
  
  val spark: SparkSession = SparkSession.builder
 .config(conf)
 .getOrCreate()
 
 var expGeoColumns = spark.sparkContext.textFile("C:\\Users\\Admin\\git\\SivaCTS\\SparkSqlDemo\\exp_geo_is_cg_columns.txt" )
 
 
 
 var expGeoColumnsStructFields = expGeoColumns.map(field => StructField(field.split(":")(0),field.split(":")(1) match {
   case "String" => StringType 
   case "Int" => IntegerType 
   case "Double" => DoubleType 
   case _ => StringType 
 }
 ))
 
 print(expGeoColumnsStructFields)
 
 
 import spark.implicits._
  
 
  
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

