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
 
  /*spark.sqlContext.sql(" select * from exp_geo_is" ).show()
  
   spark.sqlContext.sql(" select * from exp_geo_is_cg" ).show()
   * 
   */
  
  
   val dfExpGeoIs = spark.sqlContext.table("exp_geo_is")
   
   import spark.implicits._
   
   
   println(" inserting ")
   var destDf = spark.createDataFrame(dfExpGeoIs.rdd,spark.sqlContext.table("exp_geo_is_cg").schema)
   import org.apache.spark.sql.functions._
   
   destDf.
   withColumn("hh_size", 
       when( col("cv_no_pers_unit")=== "1" , "1 Person").
       when( col("cv_no_pers_unit")=== "2" , "2 Persons").
       when( col("cv_no_pers_unit")=== "3" , "3 Persons").
       when( col("cv_no_pers_unit")=== "4" , "4 Persons").
       when( col("cv_no_pers_unit")=== "5" , "5 Persons").
       when( col("cv_no_pers_unit")=== "6" , "6 Persons").
       when( col("cv_no_pers_unit")=== "7" , "7 Persons").
       when( col("cv_no_pers_unit")=== "8" , "8+ Persons").
       otherwise("Unknown"))
       
   destDf.
   write.mode("overwrite").saveAsTable("exp_geo_is_cg")
 }

