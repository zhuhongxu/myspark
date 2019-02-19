package com.zhx.dfrddtransform

import org.apache.spark.sql.SparkSession

/*
 * DataFrame和Rdd相互转换（反射的方式）
 *        先定义一个case class
 *        然后将已有的RDD转换为相应的case class的数据集
 *        最后使用Spark的隐式转换直接将RDD转换为DataFrame
 * 特点：使用起来简单，但是需要事先知道相关属性的类型
 * @Author: 遗忘的哈罗德
 * @Date: 2019-02-19 11:39
 */
object DfRddTransformDemoReflect {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("DfRddTransformDemoReflect").master("local[2]").getOrCreate()

    //构建RDD
    val infoTxtRDD = spark.sparkContext.textFile("src/main/java/com/zhx/dfrddtransform/infos.txt")

    //将infoRDD中的每一行转换为Info实体
    val infoDomainRDD = infoTxtRDD.map(_.split(",")).map(attribute => Info(attribute(0).toInt, attribute(1), attribute(2).toInt))

    //导入spark隐式转换，
    import spark.implicits._

    //RDD转换为DataFrame
    val infoDF = infoDomainRDD.toDF()

    //打印infoDF基本信息
    infoDF.printSchema()
    infoDF.show()

    //打印出年龄大于30岁的人的信息
    infoDF.filter($"age" > 30).show()

    //将infoDF注册为一个临时表，方便SparkSql操作
    infoDF.createOrReplaceTempView("infos")

    //使用SparkSql的方式打印出年龄大于30岁的人的信息
    spark.sql("select * from infos where age > 30").show()

    //释放资源
    spark.stop()
  }


  case class Info(id:Int, name:String, age:Int){

  }

}
