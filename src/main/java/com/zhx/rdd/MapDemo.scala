package com.zhx.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
 * map rdd
 * 一、pair RDD可以使用所有标准RDD上的可用的转化操作
 * @Author: 遗忘的哈罗德
 * @Date: 2019-01-22 11:34
 */
object MapDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("myspark")
    val sc = new SparkContext(conf)

//    createPairRDD(sc)
//    testReduceByKey(sc)
//    testGroupByKey(sc)
//    testMapValues(sc)
//    testFlatMapValues(sc)
//    testKeys(sc)
//    testValues(sc)
//    testSortByKey(sc)
//    testSubtractByKey(sc)
//    testJoin(sc)
//    testLeftOuterJoin(sc)
//    testRightOuterJoin(sc)
//    testFilter(sc)
//    testCountByKey(sc)
    testLookUp(sc)


  }

  /**
    * 在RDD中找到所有键对应的值
    * @param sc
    */
  def testLookUp(sc: SparkContext): Unit ={
    println(createPairRDD(sc).lookup(3).toVector)
  }

  /**
    * 每个键出现的次数
    * @param sc
    */
  def testCountByKey(sc: SparkContext): Unit ={
    println(createPairRDD(sc).countByKey())
  }

  /**
    * 过滤器
    * @param sc
    */
  def testFilter(sc: SparkContext): Unit ={
    createPairRDD(sc).filter{case (x , y) => y < 5}.foreach(println)
  }

  /**
    * RDD之间右连接
    * @param sc
    */
  def testRightOuterJoin(sc: SparkContext): Unit ={
    createPairRDD(sc).rightOuterJoin(createPairRDD2(sc)).foreach(println)
  }

  /**
    * RDD之间左连接
    * @param sc
    */
  def testLeftOuterJoin(sc: SparkContext): Unit ={
    createPairRDD(sc).leftOuterJoin(createPairRDD2(sc)).foreach(println)
  }

  /**
    * 对两个RDD进行内连接
    * @param sc
    */
  def testJoin(sc: SparkContext): Unit ={
    createPairRDD(sc).join(createPairRDD2(sc)).foreach(println)
  }

  /**
    * 删除RDD中与other RDD键相同的元素
    * @param sc
    */
  def testSubtractByKey(sc: SparkContext): Unit ={
    createPairRDD(sc).subtractByKey(createPairRDD2(sc)).foreach(println)
  }

  /**
    * 测试排序
    * @param sc
    */
  def testSortByKey(sc: SparkContext): Unit ={
    //TODO 没有达到预期效果，待进一步研究
    createPairRDD(sc).sortByKey().foreach(println)
  }

  /**
    * 获取所有的values
    * @param sc
    */
  def testValues(sc: SparkContext): Unit ={
    createPairRDD(sc).values.foreach(println)
  }

  /**
    * 获取所有的key
    * @param sc
    */
  def testKeys(sc: SparkContext): Unit ={
    createPairRDD(sc).keys.foreach(println)
  }

  /**
    * 符号化
    * @param sc
    */
  def testFlatMapValues(sc: SparkContext): Unit ={
    createPairRDD(sc).flatMapValues(_ to 5).foreach(println)
  }

  /**
    * 对pair RDD对每一个值应用一个函数而不改变键
    * @param sc
    */
  def testMapValues(sc: SparkContext): Unit ={
    createPairRDD(sc).mapValues(_ + 1).foreach(println)

  }

  /**
    * 分组
    * @param sc
    */
  def testGroupByKey(sc: SparkContext): Unit ={
    createPairRDD(sc).groupByKey().foreach(println)
  }

  /**
    * 分组累加
    * @param sc
    */
  def testReduceByKey(sc: SparkContext): Unit ={
    createPairRDD(sc).reduceByKey((x, y) => x + y).foreach(println)
  }

  /**
    * 创建一个pair RDD，以每行的第一个单词作为键，整行数据作为值
    * @param sc
    */
  def createPairRDD(sc: SparkContext) ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt3")
//    lines.map(x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt)).foreach(println)
    lines.map(x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt))
  }

  def createPairRDD2(sc: SparkContext) ={
    val lines = sc.textFile("src/main/java/com/zhx/rdd/testTxt4")
    lines.map(x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt))
  }



}
