package main

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by nhanak on Jan, 2019.    
  */
object SimpleTransformations extends App {

  val conf = new SparkConf().setMaster("local").setAppName("Simple Transform App")
  val sc = new SparkContext(conf)

  val nums = sc.parallelize(List(1,2,3,4))

  val result = nums.map(num =>  num * num)
  println("My squared nums = " + result.collect().mkString(","))

  val reduced = nums.reduce((x, y) => {
    //x is the first value on the first pass, but then the acc for the rest. Y is always the next item in the list
    println(s"x:$x + y:$y")
    x+y
  })

  println("After reducing: " + reduced)

  val folded = nums.fold(0)((x, y) => {

    println(s"fold... x:$x + y:$y")
    x + y
  })
  println("After folding:  " + folded)

  val words = sc.parallelize(List("first", "second", "third", "forth"))
  val empty = "empty"
  val resultWords = words.fold("empty")((x, y) => {
    println(s"The x word: $x, the y word $y" )
    x + y
  })

  println("resultWords: " + resultWords)

  val numCombos = sc.parallelize(List((0.5, 12), (33.22, 4), (15.87, 99)))

  val comboResult = numCombos.aggregate((0, 0))(
    (acc, combosValuePair) => {
      //acc is your initial supplied value, combosValue pair is items from the original RDD
      println(s"In first function, acc:$acc, combosValuePair:$combosValuePair")
      (acc._1 + combosValuePair._2, acc._2 + 1)
    },
    (acc1, acc2) => {
      //acc 1 is your supplied value, acc2 is the accumulated result of your first function
      println(s"In second function, acc1:$acc1 , acc2:$acc2")
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
    }
  )

  //create a copy of the numCombos RDD in memory on the driver program
  val collectedArr = numCombos.collect()


  println("Combo Result is: " + comboResult)

  System.exit(0)

}
