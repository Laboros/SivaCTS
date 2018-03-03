package example

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset


object GenerateFieldNames extends Greeting with App {
  
  val conf = new SparkConf().setAppName("test").setMaster("local")
  
  val spark: SparkSession = SparkSession.builder
 .config(conf)
 .getOrCreate()
 
 import spark.implicits._
 val dfLines = spark.sqlContext.read.textFile("C:\\Users\\Admin\\Desktop\\software\\SaiNagProjects\\Client_Siva\\ScalaTransformations\\create_tbl_exp_geo_is_cg_fields.sql" )
 dfLines.map( line => { 
   
   val result = line.split(" ")
   val fieldName =  result(0)
   val dataType = result(1)
   var scalaDataType: String = null
   if ( dataType.startsWith("decimal")){
     scalaDataType = "double"
   } else if ( dataType.startsWith("char")) {
     scalaDataType = "String"
   }else if ( dataType.startsWith("varchar")) {
     scalaDataType = "String"
   }else if ( dataType.startsWith("double")) {
     scalaDataType = "double"
   }else if ( dataType.startsWith("int")) {
     scalaDataType = "int"
   }
   fieldName.split("_")
   fieldName+": "+scalaDataType
 } ).write.csv( "C:\\Users\\Admin\\Desktop\\software\\SaiNagProjects\\Client_Siva\\ScalaTransformations\\create_tbl_exp_geo_is_cg_fields.sql_out")
 
 spark.close()
 }

