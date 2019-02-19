package com.zhx.dataframe
import org.apache.spark.sql.SparkSession

/*
 * DataFrame的常见操作
 * @Author: 遗忘的哈罗德
 * @Date: 2019-02-19 10:45
 */
object DataFrameAPI {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName("DataFrameAPI").master("local[2]").getOrCreate()

    //加载json数据，返回DataFrame
    val peopleDF = spark.read.format("json").load("src/main/java/com/zhx/dataframe/people")

    //打印peopleDF的元数据信息
    peopleDF.printSchema()

    //打印peopleDF的前20条数据
    peopleDF.show()

    //打印出所有的name
    peopleDF.select("name").show()

    //打印出所有的name，并且age + 10
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("agePlus")).show()

    //打印出年龄大于25岁的人的信息
    peopleDF.filter((peopleDF.col("age")) > 25).show()

    //打印出各个年龄段有多少人
    peopleDF.groupBy(peopleDF.col("age")).count().show()

    //释放资源
    spark.stop()


  }

}
