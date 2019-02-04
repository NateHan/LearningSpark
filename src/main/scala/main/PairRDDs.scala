package main

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by nhanak on Jan, 2019.    
  */
object PairRDDs extends App {

  val conf = new SparkConf().setMaster("local").setAppName("Simple Pair RDD App")
  val sc = new SparkContext(conf)

  val inMemPairs = Seq((12, "twelve"), (5, "five"), (77, "six"), (77, "wowee"))

  val pairRdd = sc.parallelize(inMemPairs)

  val sentences = List("here is a sentence", "are there words", "some words will be here frequently", "other ones not so much", "wow wow neat neat wow cool frequently")
  val sentenceRdd = sc.parallelize(sentences)

  val wordRdd = sentenceRdd.flatMap( sentence => sentence.split(" "))
  val wordToCount = wordRdd.map(word => (word, 1)).reduceByKey((x, y) => {
    println(s"x:$x   y:$y")
    x + y
  })

  println("final result = " + wordToCount.collect().mkString("\n"))

  val combinedKeySentences = pairRdd.combineByKey(
    value  => value, //your base level combiner value, used for the first time you see a key
    (acc:(Int, String), v:String) => (acc._1, acc._2 + v), // function used when we've seen this key before (key->value) into the key-> aggregated Value
    (acc1:(Int, String), acc2:(Int, String) => (acc1._1, acc1._2 + acc2._2))
  )

}
