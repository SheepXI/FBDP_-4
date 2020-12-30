package nju;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Stage1 {
  def main(args: Array[String]) {
    // 读取文件
    val inputFile1 = "hdfs://localhost:9000/input1/user_info_format1.csv"
    val inputFile2 = "hdfs://localhost:9000/input2/user_log_format1.csv"
    val conf = new SparkConf().setAppName("Stage1").setMaster("local")
    val sc = new SparkContext(conf)
    var textFile1 = sc.textFile(inputFile1)
    var textFile2 = sc.textFile(inputFile2)
    // 去掉header
    var header = textFile1.first()
    textFile1=textFile1.filter(row=>row!=header)
    header=textFile2.first()
    textFile2=textFile2.filter(row=>row!=header)
    // 统计最受欢迎商品
    var itemCount = textFile2.map(line => (line.split(",", -1)(1),line.split(",", -1)(5), line.split(",", -1).last)).filter(row => row._2 == "1111").filter(row => (row._3 != "0")).map(word => (word._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    itemCount=sc.parallelize(itemCount.take(100))
    itemCount.saveAsTextFile("hdfs://localhost:9000/output")
    sc.stop()
    // 统计最受年轻人欢迎商家
    /*
    val textFile3 = textFile2.map(line => (line.split(",", -1)(0),(line.split(",", -1)(3),line.split(",", -1)(5), line.split(",", -1).last))).filter(row => row._2._2 == "1111").filter(row=>row._2._3!="0")
    val textFile4=textFile1.map(line=> (line.split(",",-1)(0), line.split(",", -1)(1))).filter(row=>row._2=="1"||row._2=="2"||row._2=="3")
    // textFile的结构为(user_info,((seller_id,time_stamp,action_type),age_range)
    val textFile=textFile3.join(textFile4)
    var sellerCount=textFile.map(word => (word._2._1._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    sellerCount=sc.parallelize(sellerCount.take(100))
    sellerCount.saveAsTextFile("hdfs://localhost:9000/output")
    sc.stop()
    */
  }
}