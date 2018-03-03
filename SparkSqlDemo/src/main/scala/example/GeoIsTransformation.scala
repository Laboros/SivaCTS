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
 
 
 
 
 var expGeoCgColumns = spark.sqlContext.read.textFile("C:\\Users\\Admin\\git\\SivaCTS\\SparkSqlDemo\\exp_geo_is_cg_columns.txt" )
 
  var expGeoCgColumnsStructFields = expGeoCgColumns.rdd.map(field => field.split(":")).filter(f => f.length>1).map( split => StructField(split(0),split(1) match {
   case "String" => StringType 
   case "Int" => IntegerType 
   case "Double" => DoubleType 
   case _ => StringType 
 }
 ))
 
  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(expGeoCgColumnsStructFields.collect.toList)).createOrReplaceTempView("exp_geo_is_cg")
 
 
 var expGeoColumns = spark.sqlContext.read.textFile("C:\\Users\\Admin\\git\\SivaCTS\\SparkSqlDemo\\exp_geo_is_columns.txt" )
 
  var expGeoColumnsStructFields = expGeoColumns.rdd.map(field => field.split(":")).filter(f => f.length>1).map( split => StructField(split(0),split(1) match {
   case "String" => StringType 
   case "Int" => IntegerType 
   case "Double" => DoubleType 
   case _ => StringType 
 }
 ))
  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(expGeoColumnsStructFields.collect.toList)).createOrReplaceTempView("exp_geo_is")
 
  spark.sqlContext.sql(" select * from exp_geo_is" ).show()
  
   spark.sqlContext.sql(" select * from exp_geo_is_cg" ).show()
  
  
   val dfExpGeoIs = spark.sqlContext.table("exp_geo_is")
   
   import spark.implicits._
   
   

   spark.createDataFrame(dfExpGeoIs.rdd.map(row => row ),spark.sqlContext.table("exp_geo_is_cg").schema).write.mode("overwrite").save()
   
  
 }

