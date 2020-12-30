package nju;

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Stage2 {
  def main(args: Array[String]) {
    val inputFile1 = "hdfs://localhost:9000/input1/user_info_format1.csv"
    val inputFile2 = "hdfs://localhost:9000/input2/user_log_format1.csv"
    val conf = new SparkConf().setAppName("Stage2").setMaster("local")
    val sc = new SparkContext(conf)
    var textFile1 = sc.textFile(inputFile1)
    var textFile2 = sc.textFile(inputFile2)
    // 去掉header
    var header = textFile1.first()
    textFile1=textFile1.filter(row=>row!=header)
    header=textFile2.first()
    textFile2=textFile2.filter(row=>row!=header)
    // 合并两张表，使数据结构为(user_id,((time_stamp,action_type),(age_range,gender))，此处采用Inner Join
    val textFile3 = textFile2.map(line => (line.split(",", -1)(0),(line.split(",", -1)(5), line.split(",", -1).last))).filter(row => row._2._1 == "1111").filter(row=>row._2._2=="2")
    val textFile4=textFile1.map(line=> (line.split(",",-1)(0),(line.split(",",-1)(1),line.split(",",-1).last)))
    val textFile=textFile3.join(textFile4)
    // <18岁为1;[18,24]为2;[25,29]为3;[30,34]为4;[35,39]为5;[40,49]为6;>=50时为7和8;0和NULL表示未知
    val Count_age_range1=textFile.filter(row=>row._2._2._1=="1").count()
    val Count_age_range2=textFile.filter(row=>row._2._2._1=="2").count()
    val Count_age_range3=textFile.filter(row=>row._2._2._1=="3").count()
    val Count_age_range4=textFile.filter(row=>row._2._2._1=="4").count()
    val Count_age_range5=textFile.filter(row=>row._2._2._1=="5").count()
    val Count_age_range6=textFile.filter(row=>row._2._2._1=="6").count()
    val Count_age_range78=textFile.filter(row=>row._2._2._1=="7"||row._2._2._1=="8").count()
    val Count_age=Count_age_range1+Count_age_range2+Count_age_range3+Count_age_range4+Count_age_range5+Count_age_range6+Count_age_range78
    // 0为女，1为男
    val Count_gender_f=textFile.filter(row=>row._2._2._2=="0").count()
    val Count_gender_m=textFile.filter(row=>row._2._2._2=="1").count()
    val Count_gender=Count_gender_f+Count_gender_m

    var ans = "双十一购买商品的男女比例为：" + "\n" + "男：" + Count_gender_m.toFloat / Count_gender.toFloat * 100.toFloat + "%\n" + "女：" + Count_gender_f.toFloat / Count_gender.toFloat * 100.toFloat + "%\n" + "\n"
    ans=ans+"双十一购买商品的各年龄段比例为："+"\n"
    ans=ans+"[0,17]："+Count_age_range1.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[18,24]："+Count_age_range2.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[25,29]："+Count_age_range3.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[30,34]："+Count_age_range4.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[35,39]："+Count_age_range5.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[40,49]："+Count_age_range6.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    ans=ans+"[50,..]："+Count_age_range78.toFloat/Count_age.toFloat* 100.toFloat + "%\n"
    print(ans)
    sc.stop()
  }
}