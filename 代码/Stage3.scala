package nju;

import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.functions._


object Stage3 {

  val warehouseLocation=new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Stage3_Spark SQL")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()
    // 读取文件
    import spark.implicits._
    val inputFile1 = "hdfs://localhost:9000/input1/user_info_format1.csv"
    val inputFile2 = "hdfs://localhost:9000/input2/user_log_format1.csv"
    val userDF=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load(inputFile1)
    userDF.createTempView("user_info")
    val logDF=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load(inputFile2)
    logDF.createTempView("user_log")
    // 筛选时间戳和action_type
    val logDF2=spark.sql("SELECT a.user_id,a.action_type FROM user_log a WHERE a.action_type=2 AND a.time_stamp=1111")
    logDF2.createTempView("filtered_log")
    // 合并info和log两张表，此处采用Inner Join
    val sqlDF=spark.sql("SELECT a.*,b.age_range,b.gender FROM filtered_log a JOIN user_info b ON a.user_id = b.user_id")
    sqlDF.createTempView("FinalTable")
    // 计算比例
    var Count_gender = sqlDF.groupBy($"gender").count()
    Count_gender=Count_gender.filter($"gender"===1 || $"gender"===0)
    Count_gender=Count_gender.withColumn("percent(%)",$"count".divide(sum($"count").over()).multiply(100))
    var Count_age_range = sqlDF.groupBy($"age_range").count()
    Count_age_range=Count_age_range.filter($"age_range">0 && $"age_range"<9)
    Count_age_range=Count_age_range.withColumn("percent(%)",$"count".divide(sum($"count").over()).multiply(100))
    Count_age_range=Count_age_range.sort($"age_range")
    // 输出到文件
    Count_gender.write.option("header","true").csv("hdfs://localhost:9000/output1/gender_proportion")
    Count_age_range.write.option("header","true").csv("hdfs://localhost:9000/output1/age_proportion")
  }
}
