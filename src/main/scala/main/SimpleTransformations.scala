package main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


/**
  * created by nhanak on Jan, 2019.    
  */
object SimpleTransformations extends App {

  val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.master", "local").getOrCreate()

  val conf = new SparkConf().setMaster("local").setAppName("Simple Transform App")
  val sc = new SparkContext(conf)

  val lines = sc.

}
