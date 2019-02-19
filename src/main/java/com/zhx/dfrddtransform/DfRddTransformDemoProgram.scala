package com.zhx.dfrddtransform

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/*
 * DataFrame和Rdd相互转换（编码的方式）：
 *        1.create an RDD of Rows from an original RDD
 *        2.create the schema represented by a StructType matching the structure of records in the RDD created in step 1
 *        3.apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession
 *        先构建schema，然后将构建的schema作用于已有的RDD之上
 * 特点：代码比较多，比较适用于事先不知道相关属性的类型，必须在运行期间才知道的情况
 * @Author: 遗忘的哈罗德
 * @Date: 2019-02-19 14:40
 */
object DfRddTransformDemoProgram {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("DfRddTransformDemoProgram").master("local[2]").getOrCreate()

    //构建RDD
    val infoTxtRDD = spark.sparkContext.textFile("src/main/java/com/zhx/dfrddtransform/infos.txt")

    //将infoRDD转换为RowRDD
    val infoRowRDD = infoTxtRDD.map(_.split(",")).map(attribute => Row(attribute(0), attribute(1), attribute(2)))

    //定义schema信息
    val structType = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("age", StringType, true)
    ))

    //创建DataFrame
    val infoDF = spark.createDataFrame(infoRowRDD, structType)

    //创建临时表
    infoDF.createOrReplaceTempView("info")

    //spark sql查询
    spark.sql("select * from info").show()
    spark.sql("select * from info where age > 20").show()

    //释放资源
    spark.stop()
  }

}
