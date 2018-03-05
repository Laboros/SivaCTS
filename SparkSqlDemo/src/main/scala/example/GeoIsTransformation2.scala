package example

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column




object GeoIsTransformation2 extends App {
  
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
   
   val deriveHHSize = ( cvNoPersUnit : Column) => when( cvNoPersUnit=== "1" , "1 Person").
       when( cvNoPersUnit=== "2" , "2 Persons").
       when( cvNoPersUnit=== "3" , "3 Persons").
       when( cvNoPersUnit=== "4" , "4 Persons").
       when( cvNoPersUnit=== "5" , "5 Persons").
       when( cvNoPersUnit=== "6" , "6 Persons").
       when( cvNoPersUnit=== "7" , "7 Persons").
       when( cvNoPersUnit=== "8" , "8+ Persons").
       otherwise("Unknown")
       
         
  /* val generateLookupCheck = ( anyColumn : Column , kvMap : Map[Any,Any], otherwise : Any) => {
     
     var outCol : Column = null
     
     kvMap.foreach(kv => case out when(anyColumn.===(kv._1),kv._2)
   }*/
     
 
   val deriveCountrySize = ( urbanCountryCode : Column) => when( urbanCountryCode=== "1" , "1 Metro Pop 1M+").
       when( urbanCountryCode=== "2" , "2 Metro Pop 250K - 1M").
       when( urbanCountryCode=== "3" , "3 Metro Pop 250K").
       when( urbanCountryCode=== "4" , "4 Urban 20K Metro adjacent").
       when( urbanCountryCode=== "5" , "5 Urban 20K not adjacent").
       when( urbanCountryCode=== "6" , "6 Urban 2500 - 19999 adjacent").
       when( urbanCountryCode=== "7" , "7 Urban 2500 - 19999 not adjacent").
       when( urbanCountryCode=== "8" , "8 Rural < 2500 urban adjacent").
       when( urbanCountryCode=== "9" , "9 Rural < 2500 urban not adjacent").
       otherwise("Unknown")    
       
   
   val getHHPrimePersonExpression = "cv_p2_ptyp='P' and cv_p2_gender='F'"
   
   val determineAge = (p2Age : Column, p1Age : Column) => when(expr(getHHPrimePersonExpression)===true,substring(p2Age,2,2)).otherwise(substring(p1Age,2,2)).cast(IntegerType)

   val identifyEthnicity = (p2EtncGrp : Column, p1EtncGrp : Column) => when(expr(getHHPrimePersonExpression)===true,p2EtncGrp).otherwise(p1EtncGrp)

  

   
   val deriveHHAge = ( p2Age : Column, p1Age : Column) => 
     {
       var exprColumn = determineAge(p2Age,p1Age)
       when(exprColumn.between(19, 30),"01 Millennial (Age 19-39)").
       when(exprColumn.between(40, 50),"02 Gen X (Age 40-50)").
       when(exprColumn.between(51, 60),"03 Young Boomer (Age 51-60)").
       when(exprColumn.between(61, 70),"04 Older Boomer (61-70)").
       when(exprColumn.between(71, 80),"05 Retirees (Age 71-80)").
       when(exprColumn>81,"06 Seniors (Age 81+)")
       .otherwise("Unknown")
     }
     
     val determinePrimaryPersonGenderWithGivenAge = ( gender : String , p2Gender : Column, p2Age:Column, p1Gender : Column, p1Age:Column,low : Any , high : Any) => {
       when(determineAge(p2Age,p1Age).between(low, high).and(when(expr(getHHPrimePersonExpression)===true,p2Gender).otherwise(p1Gender).===(gender))===true,"Yes").
       otherwise("No")
       
     }
 
     val determinePrimaryPersonGenderWithGivenMaxAge = ( gender : String , p2Gender : Column, p2Age:Column, p1Gender : Column, p1Age:Column,low : Any ) => {
       when(determineAge(p2Age,p1Age).>(low).and(when(expr(getHHPrimePersonExpression)===true,p2Gender).otherwise(p1Gender).===(gender))===true,"Yes").
       otherwise("No")
       
     }
 
     val applyChildPresTypes = ( anyColumn : Column,cvChildPres0_18 :Column) => anyColumn.and(cvChildPres0_18.isin("00","0U","5N","5U")) 
     
     val applyChildAgeChecks = ( col : Column ) => col.isin("1Y","5Y")
       
       
     
     val deriveHHLifeStage = ( cvP2ComAge : Column, cvP1ComAge : Column,cvChildPres0_18 :Column) => 
     {
       var exprColumn = determineAge(cvP2ComAge,cvP1ComAge)
       when(applyChildPresTypes(exprColumn.<=(45),cvChildPres0_18),"01 Getting Started").
       when(applyChildPresTypes(exprColumn.between(46,70),cvChildPres0_18),"04 Established Workers").
       when(applyChildPresTypes(exprColumn.>=(71),cvChildPres0_18),"05 Retired").
       when( applyChildAgeChecks(destDf("cv_child_age_0_3_mdl")).or(applyChildAgeChecks(destDf("cv_child_age_4_6_mdl"))).
           or(applyChildAgeChecks(destDf("cv_child_age_7_9_mdl"))).or(applyChildAgeChecks(destDf("cv_child_age_10_12_mdl"))),"02 Younger Families").
       when( applyChildAgeChecks(destDf("cv_child_age_13_15_mdl")).or(applyChildAgeChecks(destDf("cv_child_age_13_15_mdl"))),"03 Raising Teens")
       .otherwise("Unknown")
     }

    val determineHHEthnicity = (cvP2EtncGrp : Column, cvP1EtncGrp : Column) => {
     val ethnicity = identifyEthnicity(cvP2EtncGrp,cvP1EtncGrp)
     when(ethnicity.isin("E","G","J","K","L"),"White, Non-Hisp").
     when(ethnicity.isin("A"),"African American, Non-Hisp").
     when(ethnicity.isin("B","C","D","H","N"),"Asian, Non-Hisp").
     when(ethnicity.isin("O"),"Hispanic").
     otherwise("Other/Unknown, Non-Hisp")
   }
    
    val determineHHIncome = (amount : Column) => {
      val intAmount = amount.cast(DoubleType)
     when(intAmount.between(1,15),"$0-$14.9999K").
     when(intAmount.between(15,24.9999),"$15K-$24.9999K").
     when(intAmount.between(25,34.9999),"$25K-$34.9999K").
     when(intAmount.between(35,49.9999),"$35K-$49.9999K").
     when(intAmount.between(50,69.9999),"$50K-$69.9999K").
     when(intAmount.between(70,99.9999),"$70K-$99.9999K").
     when(intAmount.>=(100),"$100K+").
     otherwise("Unknown")  
   }
    
    val determineHighLowMediumPCI = ( amount : Column , low : Int, high : Int) => when(amount.<(low),"Low").when(amount>(high),"High").otherwise("Medium")
    
     val determineHHPerCapitaIncome = (amount : Column , numberOfPersons : Column) => {
      val intAmount = amount.cast(IntegerType)
     when(numberOfPersons==="1",determineHighLowMediumPCI(intAmount,25,50)).
     when(numberOfPersons==="2",determineHighLowMediumPCI(intAmount,35,60)).
     when(numberOfPersons==="3",determineHighLowMediumPCI(intAmount,45,70)).
     when(numberOfPersons==="4",determineHighLowMediumPCI(intAmount,55,80)).
     when(numberOfPersons==="5",determineHighLowMediumPCI(intAmount,65,90)).
     when(numberOfPersons==="6",determineHighLowMediumPCI(intAmount,75,100)).
     when(numberOfPersons==="7",determineHighLowMediumPCI(intAmount,85,110)).
     when(numberOfPersons==="8",determineHighLowMediumPCI(intAmount,95,120)).
     otherwise("Unknown")  
   }
    
   val determineAgeOfOldChild = () => {
      when(
       applyChildAgeChecks(destDf("cv_child_age_13_15_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_16_18_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Oldest Child 13-18").
       when(
       applyChildAgeChecks(destDf("cv_child_age_7_9_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_10_12_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Oldest Child 7-12") 
       when(
       applyChildAgeChecks(destDf("cv_child_age_0_3_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_4_6_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Oldest Child 0-6")
       .otherwise("No Children")
   }  
     
   destDf.
   withColumn("hh_size", deriveHHSize(destDf("cv_no_pers_unit"))) 
   .withColumn("hoh_age", deriveHHAge(destDf("cv_p2_com_age"),destDf("cv_p1_com_age")))
   .withColumn("hoh_lifestage", deriveHHLifeStage(destDf("cv_p2_com_age"),destDf("cv_p1_com_age"),destDf("cv_child_pres_0_18")))
   .withColumn("hoh_ethnicity", determineHHEthnicity(destDf("cv_p2_etnc_grp"),destDf("cv_p1_etnc_grp")))
   .withColumn("hoh_hispanic", identifyEthnicity(destDf("cv_p2_etnc_grp"),destDf("cv_p1_etnc_grp")).isin("O"))
   .withColumn("hh_income", determineHHIncome(destDf("cv_est_inc_amtv5")))
   .withColumn("hh_per_cap_income", determineHHPerCapitaIncome(destDf("cv_est_inc_amtv5"),destDf("cv_no_pers_unit")))
   .withColumn("hh_presence_of_child", when(applyChildAgeChecks(destDf("cv_child_pres_0_18")).and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Yes").otherwise("No"))
   //To Verify
   .withColumn("hh_presence_of_child_age_0_6", when(
       applyChildAgeChecks(destDf("cv_child_age_0_3_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_4_6_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Yes").otherwise("No"))
       
   .withColumn("hh_presence_of_child_age_7_12", when(
       applyChildAgeChecks(destDf("cv_child_age_7_9_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_10_12_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Yes").otherwise("No"))
       
   .withColumn("hh_presence_of_child_age_13_18", when(
       applyChildAgeChecks(destDf("cv_child_age_13_15_mdl")).
       or(applyChildAgeChecks(destDf("cv_child_age_16_18_mdl"))).
       and(destDf("cv_no_child_unit").cast(IntegerType).>=(1)),"Yes").otherwise("No"))
       
   .withColumn("hh_age_of_old_child", determineAgeOfOldChild())
   //.withColumn("hh_presence_of_gender",)
   //.withColumn("hh_millennial_male",)
   //.withColumn("hh_millennial_male",)
   //.withColumn("hh_millennial_male",)
   //.withColumn("hh_millennial_male",)
   //.withColumn("hh_millennial_male",)
   .withColumn("hh_millennial_male", determinePrimaryPersonGenderWithGivenAge("M" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),19,39))   
   .withColumn("hh_millennial_female", determinePrimaryPersonGenderWithGivenAge("F" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),19,39))   
   .withColumn("hh_gen_x_male", determinePrimaryPersonGenderWithGivenAge("M" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),40,50))   
   .withColumn("hh_gen_x_female", determinePrimaryPersonGenderWithGivenAge("F" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),40,50))   
   .withColumn("hh_young_boomer_male", determinePrimaryPersonGenderWithGivenAge("M" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),51,60))   
   .withColumn("hh_young_boomer_female", determinePrimaryPersonGenderWithGivenAge("F" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),51,60))   
   .withColumn("hh_post_boomer_male", determinePrimaryPersonGenderWithGivenMaxAge("M" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),61))   
   .withColumn("hh_post_boomer_female", determinePrimaryPersonGenderWithGivenMaxAge("F" , destDf("cv_p2_gender"), destDf("cv_p2_com_age"), destDf("cv_p1_gender"), destDf("cv_p1_com_age"),61))   

   .withColumn("hh_county_size", deriveCountrySize(destDf("cv_cen_rur_urban_county_szcd")))   
   .withColumn("hh_urban_suburban_rural", when(destDf("cv_cbsa_type")==="A","Urban").
       when(destDf("cv_cbsa_type")==="B","Suburban").
       when(destDf("cv_cbsa_type")==="C","Rural").otherwise("Unknown"))
   .withColumn("HH_County", concat(destDf("cv_county_cd"),lit("-"),destDf("cv_county_name")))
   
   destDf.
   write.mode("overwrite").saveAsTable("exp_geo_is_cg")
 }

