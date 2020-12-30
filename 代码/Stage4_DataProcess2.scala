package nju

import java.io.File

import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
;

object DataProcess2 {
  val warehouseLocation=new File("spark-warehouse").getAbsolutePath
  val inputFile1 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成3/DataProcess1.csv"
  val inputFile2 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/train_format1.csv"
  val inputFile3 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/test_format1.csv"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stage4").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .appName("Stage4")
      .master("local[8]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.default.parallelism", 1000)
      .config("spark_sql_shuffle_partitions", 1000)
      .config("spark_num_executors", 125)
      .config("spark_executor_cores", 8)
      .getOrCreate()
    // 读取文件
    import spark.implicits._
    val userDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile1)
    userDF.createOrReplaceTempView("all_user_data")
    var trainDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile2)
    trainDF.createOrReplaceTempView("train_data")
    var testDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile3)
    testDF.createOrReplaceTempView("test_data")

    // 给训练集/测试集匹配特征，对user_id,seller_id做独热处理
    trainDF=spark.sql("SELECT b.*,a.label FROM train_data a LEFT JOIN all_user_data b ON (a.user_id=b.user_id AND a.merchant_id=b.seller_id)")
    var indexer = new StringIndexer().setInputCol("user_id").setOutputCol("user_id_Index").fit(trainDF)
    var indexed = indexer.transform(trainDF)
    var encoder = new OneHotEncoder().setInputCol("user_id_Index").setOutputCol("user_id_Vec").setDropLast(false)
    var encoded = encoder.transform(indexed)
    trainDF=encoded
    indexer = new StringIndexer().setInputCol("seller_id").setOutputCol("seller_id_Index").fit(trainDF)
    indexed = indexer.transform(trainDF)
    encoder = new OneHotEncoder().setInputCol("seller_id_Index").setOutputCol("seller_id_Vec").setDropLast(false)
    encoded = encoder.transform(indexed)
    trainDF=encoded
    trainDF.show(5)
    trainDF.coalesce(1).write.option("header","true").json("/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成10")

    testDF=spark.sql("SELECT b.* FROM test_data a LEFT JOIN all_user_data b ON (a.user_id=b.user_id AND a.merchant_id=b.seller_id)")
    indexer = new StringIndexer().setInputCol("user_id").setOutputCol("user_id_Index").fit(testDF)
    indexed = indexer.transform(testDF)
    encoder = new OneHotEncoder().setInputCol("user_id_Index").setOutputCol("user_id_Vec").setDropLast(false)
    encoded = encoder.transform(indexed)
    testDF=encoded
    indexer = new StringIndexer().setInputCol("seller_id").setOutputCol("seller_id_Index").fit(testDF)
    indexed = indexer.transform(testDF)
    encoder = new OneHotEncoder().setInputCol("seller_id_Index").setOutputCol("seller_id_Vec").setDropLast(false)
    encoded = encoder.transform(indexed)
    testDF=encoded
    testDF.show(5)
    testDF.coalesce(1).write.option("header","true").json("/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完11")

    spark.stop()
  }
}
