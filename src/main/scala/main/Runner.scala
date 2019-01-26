package main

import org.apache.spark.sql.SparkSession

/**
  * created by nhanak on Dec, 2018.    
  */
object Runner extends App {

  val fileParentDir = "/Users/nhanak/IdeaProjects/SparkLearning/TestResources/" // Should be some file on your system
  val fileName = "TextFile5000Rows6Columns.txt"
//  val fileName = "TextFile5000000Rows4Columns.txt"
  val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.master", "local").getOrCreate()
  val logData = spark.read.textFile(fileParentDir + fileName).cache()
  val aCount = logData.filter(line => line.contains("a")).count()
  println(s"done, aCount is: $aCount")
  val bCount = logData.filter(line => line.contains("b")).count()
  println(s"done, bCount is: $bCount")

  println(s"Finished job, a count = $aCount b count = $bCount")

  spark.stop()
  System.exit(1)
}
