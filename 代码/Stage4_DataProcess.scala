package nju;

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

object DataProcess {

  val warehouseLocation=new File("spark-warehouse").getAbsolutePath
  val inputFile1 = "hdfs://localhost:9000/input1/user_info_format1.csv"
  val inputFile2 = "hdfs://localhost:9000/input2/user_log_format1.csv"
  val inputFile3 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/train_format1.csv"
  val inputFile4 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/test_format1.csv"
  val inputFile5 = "/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成/DataProcess1.csv"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stage4").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession
      .builder()
      .appName("Stage4")
      .master("local[8]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.default.parallelism",1000)
      .config("spark_sql_shuffle_partitions",1000)
      .config("spark_num_executors",125)
      .config("spark_executor_cores",8)
      .getOrCreate()
    // 读取文件
    import spark.implicits._
    var userDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load(inputFile1)
    userDF = userDF.withColumn("gender", when($"gender"===2,null).otherwise($"gender"))
    userDF = userDF.withColumn("age_range", when($"age_range"===0,null).otherwise($"age_range"))
    userDF.createTempView("user_info")
    val logDF=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load(inputFile2)
    logDF.createTempView("user_log")
    // 合并info和log两张表，此处采用Left Join
    var all_user_data = spark.sql("SELECT a.*,b.age_range,b.gender FROM user_log a LEFT JOIN user_info b ON a.user_id = b.user_id")
    all_user_data.createOrReplaceTempView("all_user_data")
    // 处理数据
    // 某用户对某商家进行过几次action
    var Count_seller = all_user_data.groupBy($"user_id",$"seller_id").count()
    Count_seller=Count_seller.withColumnRenamed("count", "user_view_seller_count")
    var FinalDF=spark.sql("SELECT user_id,seller_id,age_range,gender,action_type FROM all_user_data")
    FinalDF.createOrReplaceTempView("Final")
    // 某用户对某商家各种action_type的计数
    var tempDF=Count_seller
    tempDF.createOrReplaceTempView("temp")
    val action0_count=spark.sql("SELECT DISTINCT user_id,seller_id,count(action_type) view_count FROM Final WHERE action_type=0 GROUP BY user_id,seller_id")
    action0_count.createOrReplaceTempView("action0")
    tempDF=spark.sql("SELECT a.*,b.view_count FROM temp a LEFT JOIN action0 b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    tempDF.createOrReplaceTempView("temp")
    val action1_count=spark.sql("SELECT DISTINCT user_id,seller_id,count(action_type) addcart_count FROM Final WHERE action_type=1 GROUP BY user_id,seller_id")
    action1_count.createOrReplaceTempView("action1")
    tempDF=spark.sql("SELECT a.*,b.addcart_count FROM temp a LEFT JOIN action1 b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    tempDF.createOrReplaceTempView("temp")
    val action2_count=spark.sql("SELECT DISTINCT user_id,seller_id,count(action_type) buy_count FROM Final WHERE action_type=2 GROUP BY user_id,seller_id")
    action2_count.createOrReplaceTempView("action2")
    tempDF=spark.sql("SELECT a.*,b.buy_count FROM temp a LEFT JOIN action2 b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    tempDF.createOrReplaceTempView("temp")
    val action3_count=spark.sql("SELECT DISTINCT user_id,seller_id,count(action_type) star_count FROM Final WHERE action_type=3 GROUP BY user_id,seller_id")
    action3_count.createOrReplaceTempView("action3")
    tempDF=spark.sql("SELECT a.*,b.star_count FROM temp a LEFT JOIN action3 b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    tempDF.createOrReplaceTempView("temp")
    tempDF=tempDF.sort("user_id","seller_id")
    tempDF.show(15)
    FinalDF=tempDF

    // 统计购买/所有action和购买/（加购物车+购买+加收藏）的百分比
    FinalDF=FinalDF.withColumn("buy_ratio",$"buy_count".divide($"user_view_seller_count").multiply(100))
    FinalDF=FinalDF.withColumn("buy_ratio2",$"buy_count".divide($"user_view_seller_count".minus($"view_count")).multiply(100))
    FinalDF = FinalDF.withColumn("buy_ratio2", when($"buy_ratio"===100,100).otherwise($"buy_ratio2"))
    FinalDF.show(15)
    FinalDF.createOrReplaceTempView("Final")
    all_user_data=spark.sql("SELECT a.*,b.buy_ratio FROM all_user_data a LEFT JOIN Final b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    all_user_data.createOrReplaceTempView("all_user_data")

    // 某商家总共被操作过多少次
    var Count_seller_viewed=all_user_data.groupBy($"seller_id").count()
    Count_seller_viewed=Count_seller_viewed.withColumnRenamed("count", "seller_viewed_count")
    Count_seller_viewed.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*,b.seller_viewed_count FROM Final a LEFT JOIN temp b ON a.seller_id=b.seller_id")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF.show(5)
    // 某商家购买者性别统计
    var Count_seller_female=spark.sql("SELECT DISTINCT seller_id,count(gender) female_count FROM all_user_data WHERE gender=0 AND buy_ratio is not null GROUP BY seller_id")
    Count_seller_female.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*, b.female_count FROM Final a LEFT JOIN temp b ON a.seller_id=b.seller_id")
    FinalDF.createOrReplaceTempView("Final")
    var Count_seller_male=spark.sql("SELECT DISTINCT seller_id,count(gender) male_count FROM all_user_data WHERE gender=1 AND buy_ratio is not null GROUP BY seller_id")
    Count_seller_male.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*, b.male_count FROM Final a LEFT JOIN temp b ON a.seller_id=b.seller_id")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF=FinalDF.withColumn("female_ratio",$"female_count".divide($"female_count".plus($"male_count")).multiply(100))
    FinalDF=FinalDF.withColumn("male_ratio",$"male_count".divide($"female_count".plus($"male_count")).multiply(100))
    FinalDF.createOrReplaceTempView("Final")

    // 用户在某商家看过多少种商品/商品类别/品牌
    var Count_item_viewed=all_user_data.groupBy($"user_id",$"seller_id",$"item_id").count().sort("user_id","seller_id")
    Count_item_viewed=Count_item_viewed.groupBy($"user_id",$"seller_id").count().sort("user_id","seller_id")
    Count_item_viewed=Count_item_viewed.withColumnRenamed("count", "item_viewed_count")
    Count_item_viewed.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*,b.item_viewed_count FROM Final a LEFT JOIN temp b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF.show(5)

    var Count_cat_viewed=all_user_data.groupBy($"user_id",$"seller_id",$"cat_id").count()
    Count_cat_viewed=Count_cat_viewed.groupBy($"user_id",$"seller_id").count().sort("user_id","seller_id")
    Count_cat_viewed=Count_cat_viewed.withColumnRenamed("count", "cat_viewed_count")
    Count_cat_viewed.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*,b.cat_viewed_count FROM Final a LEFT JOIN temp b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF.show(5)

    var Count_brand_viewed=all_user_data.groupBy($"user_id",$"seller_id",$"brand_id").count()
    Count_brand_viewed=Count_brand_viewed.groupBy($"user_id",$"seller_id").count().sort("user_id","seller_id")
    Count_brand_viewed=Count_brand_viewed.withColumnRenamed("count", "brand_viewed_count")
    Count_brand_viewed.createOrReplaceTempView("temp")
    FinalDF=spark.sql("SELECT a.*,b.brand_viewed_count FROM Final a LEFT JOIN temp b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF.show(5)

    FinalDF.coalesce(1).write.option("header","true").csv("/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成")

    /*
    var FinalDF=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load(inputFile5)
    FinalDF.createOrReplaceTempView("Final")
    val Var_time=spark.sql("SELECT DISTINCT user_id,seller_id,variance(time_stamp) time_var FROM all_user_data GROUP BY user_id,seller_id")
    Var_time.createOrReplaceTempView("var_time_view")
    FinalDF=spark.sql("SELECT a.*,b.time_var FROM Final a LEFT JOIN var_time_view b ON (a.user_id=b.user_id AND a.seller_id=b.seller_id)")
    FinalDF.createOrReplaceTempView("Final")
    FinalDF=spark.sql("SELECT a.*,b.age_range,b.gender FROM Final a LEFT JOIN user_info b ON a.user_id=b.user_id")
    FinalDF.coalesce(1).write.option("header","true").csv("/C:/Users/Alienware/Desktop/FBDP_实验四_席晓阳_171840013/数据/数据处理完成3")*/
    sc.stop()
  }
}