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

    testWordCount(sc)


  }

  /**
    * wordCount程序
    * @param sc
    */
  def testWordCount(sc: SparkContext): Unit ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/wordCountData")
    lines.flatMap(_.split(" ")).map((_ , 1)).reduceByKey(_ + _).foreach(println)
    sc.stop()
  }



}
