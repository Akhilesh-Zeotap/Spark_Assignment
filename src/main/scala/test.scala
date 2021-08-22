import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object test extends App {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._
  val df1 = Seq(
    (1, "bat"),
    (2, "mouse"),
    (3, "horse"),
    (4, "snake")
  ).toDF("id", "word")

  val df2 = Seq(
    (5, "dog"),
    (2, "cat"),
    (3, "crane"),
    (6, "elephant")
  ).toDF("id", "word")

  //val df = df1.join(df2,df1("id")===df2("id"),"inner")
  //val df = df1.join(df2,df1("id")=!=df2("id"),"cross")
  val df = df2.union(df1).dropDuplicates("id")
  df.show()

}
