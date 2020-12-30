package  nju;

import java.io.File

import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import ml.dmlc.xgboost4j.scala.spark.{TrackerConf, XGBoostClassificationModel, XGBoostClassifier}
import ml.dmlc.xgboost4j.scala.XGBoost
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.when
import org.apache.spark.rdd._
import org.spark_project.dmg.pmml.True

object ModelBuild {
  val warehouseLocation=new File("spark-warehouse").getAbsolutePath
  val inputFile1 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成8/train_data.csv"
  val inputFile2 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成9/test_data.csv"
  val inputFile3 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成10/train_data.json"
  val inputFile4 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成11/test_data.json"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stage4").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .appName("Stage4")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()
    // 读取文件
    import spark.implicits._
    var trainDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile1)
    trainDF.createOrReplaceTempView("train_data")
    trainDF=trainDF.withColumn("time_var", when($"time_var"==="NaN",null).otherwise($"time_var".cast("Double")))
    trainDF=trainDF.na.fill(-1)
    // 采样
    val trainDF1=trainDF.where($"label"===1)
    val trainDF0=trainDF.where($"label"===0).sample(true,0.2,seed=2020)
    trainDF = trainDF0.union(trainDF1)
    trainDF.show(5)

    var testDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile2)
    testDF.createOrReplaceTempView("test_data")
    testDF=testDF.withColumn("time_var", when($"time_var"==="NaN",null).otherwise($"time_var".cast("Double")))
    testDF=testDF.na.fill(-1)

    // 配置参数
    val paramMap = Map(
      "eta" -> 0.15,
      "objective" -> "binary:logistic",
      "early_stopping_rounds"->50,
      "eval_metric"->"auc",
      "alpha"->1,
      "process_type"->"update",
      "tracker_conf" -> TrackerConf(1 * 60 * 1000,"scala"))

    //user_view_seller_count	view_count	addcart_count	buy_count	star_count	buy_ratio	buy_ratio2	seller_viewed_count	female_count	male_count	female_ratio	male_ratio	item_viewed_count	cat_viewed_count	brand_viewed_count	time_var	age_range	gender
    val names=Array("user_view_seller_count","view_count","addcart_count","buy_count","star_count","buy_ratio","buy_ratio2","seller_viewed_count","female_count","male_count","female_ratio","male_ratio","item_viewed_count","cat_viewed_count","brand_viewed_count","time_var","age_range","gender")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(names)
      .setOutputCol("features")
    //  .setHandleInvalid("keep")

    val XgbTrainInput = vectorAssembler.transform(trainDF).select("features","label")
    val XgbTestInput = vectorAssembler.transform(testDF).select("features","user_id","seller_id")
    XgbTrainInput.show()
    XgbTestInput.show()
    val xgbClassifier = new XGBoostClassifier(paramMap).setFeaturesCol("features").setLabelCol("label")
    xgbClassifier.setMaxDepth(10)
    xgbClassifier.setNumRound(750)
    val xgbClassificationModel = xgbClassifier.fit(XgbTrainInput)
    val predict = xgbClassificationModel.transform(XgbTestInput)
    predict.show()
    predict.createOrReplaceTempView("predict")
    testDF=spark.sql("SELECT b.user_id,b.seller_id as merchant_id,b.prediction as prob FROM test_data a LEFT JOIN predict b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    testDF.show()
    testDF.coalesce(1).write.option("header","true").csv("/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/预测结果10")
  }
}