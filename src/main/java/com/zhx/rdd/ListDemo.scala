package com.zhx.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
 * list rdd
 * @Author: 遗忘的哈罗德
 * @Date: 2019-01-18 17:46
 */
object ListDemo {

  def main(args: Array[String]): Unit = {
    //独立应用中获取sparkContext的方法：
    val conf = new SparkConf().setMaster("local").setAppName("myspark")
    val sc = new SparkContext(conf)

//    testWordCount(sc)
//    testFilter(sc)
//    testUnion(sc)
//    testCount(sc)
//    testTake(sc)
//    testMap(sc)
//    testFlatMap(sc)
//    testReduce(sc)
      testAggregate(sc)

  }

  def testAggregate(sc: SparkContext): Unit ={
    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    println(input.aggregate(1)(_+_, _+_))
  }



  /**
    * 测试reduce
    * @param sc
    */
  def testReduce(sc: SparkContext): Unit ={
    println(sc.parallelize(1 to 10).reduce(_+_))

  }

  /**
    * 测试flatMap
    * @param sc
    */
  def testFlatMap(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    lines.flatMap(_.split(" ").toVector).foreach(println)
  }

  /**
    * 测试map
    * @param sc
    */
  def testMap(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    lines.map(_.split(" ").toVector).foreach(println)
  }

  /**
    * 测试take
    * @param sc
    */
  def testTake(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    val error = lines.filter(line => line.contains("error"))
    error.take(1).foreach(println)
  }

  /**
    * 测试记数
    * @param sc
    */
  def testCount(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    val error = lines.filter(line => line.contains("error"))
    println(error.count())
  }

  /**
    * 测试合并
    * @param sc
    */
  def testUnion(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    val error = lines.filter(line => line.contains("error"))
    val warn = lines.filter(line => line.contains("warn"))
    error.union(warn).foreach(println)
  }

  /**
    * 测试过滤器
    * @param sc
    */
  def testFilter(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")

    //打印出文本文件中所有包含error的行
    lines.filter(_.contains("error")).foreach(println)
  }

  /**
    * wordCount程序
    * @param sc
    */
  def testWordCount(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt")
    lines.flatMap(_.split(" ")).map((_ , 1)).reduceByKey(_ + _).foreach(println)
    sc.stop()
  }



}
