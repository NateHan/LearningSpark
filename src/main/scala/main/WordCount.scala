package main

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by nhanak on Dec, 2018.    
  */
object WordCount extends App {

  val conf = new SparkConf().setMaster("local").setAppName("WordCount_App")
  val sc = new SparkContext(conf)

  val projectDir = System.getProperty("user.dir")
  val localSourcesPath = "/TestResources/TestTextFiles/"

  val input = sc.textFile(projectDir + localSourcesPath + "CountFile.txt")
  val words = input.flatMap( line => line.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey{ case (x, y) => x + y}

  counts.saveAsTextFile(projectDir + localSourcesPath + "CountFile")

}
