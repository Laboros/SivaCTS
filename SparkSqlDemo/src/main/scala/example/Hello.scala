package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Hello extends Greeting with App {
  
  System.setProperty("hadoop.home.dir", "C:\\Users\\Admin\\test\\winutils_bin")
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf) // An existing SparkContext.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._
  val data = Array(1, 2, 3, 4, 5)                     // create Array of Integers
    val dataRDD = sc.parallelize(data)                // create an RDD
    //dataRDD.saveAsTextFile("C:\\Users\\Admin\\test\\data\\t1.csv")
    val dataDF = dataRDD.toDF()                         // convert RDD to DataFrame
    dataDF.write.format("com.databricks.spark.csv").save("t2.csv")               // write to parquet
     val newDataDF = sqlContext.
                read.csv("t2.csv")        // read back parquet to DF
    newDataDF.show()  
    sc.stop()
}

trait Greeting {
  lazy val greeting: String = "hello"
}
