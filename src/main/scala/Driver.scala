import MergeDF.mergeDF
import PriorityGenerator.generatePriority
import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Driver {

  /*
  Returns the count of the overlapping rows in 2 dataframes using "Identifier" column.
   */
  def countOverlap(df1: DataFrame, df2: DataFrame): Long = {
    df1.join(df2, "Identifier").count()
  }

  /*
  Returns age count for a particular DP Id.
  It will be same as count of DP Id in dataframe.
   */
  def dpAgeCount(df: DataFrame, DpId: Int): Any = {
    getCount(df, DpId)
  }

  /*
  Returns Gender count for a particular DP Id.
  It will be same as count of DP Id in dataframe.
   */
  def dpGenderCount(df: DataFrame, DpId: Int): Any = {
    getCount(df, DpId)
  }

  /*
  Returns Zip Code count for a particular DP Id.
  It will be same as count of DP Id in dataframe.
   */
  def dpZipCodeCount(df: DataFrame, DpId: Int): Any = {
    getCount(df, DpId)
  }

  /*
  Returns the count of rows in a dataframe for a particular DpId.
   */
  def getCount(df: DataFrame, DpId: Int): Any = {
    val res = df.groupBy("DpId")
      .count()
      .filter(x => x(0) == DpId)
      .head
    res(1)
  }

  /*
  Returns number of distinct Identifiers in Dataframe.
  All Identifiers are unique in any Dataframe, so it will be count of rows in dataframe.
   */
  def identifierCount(df: DataFrame): Long = {
    df.count()
  }

  def ageCount(df: DataFrame): DataFrame = {
    df.groupBy("Age").count()
  }

  def genderCount(df: DataFrame): DataFrame = {
    df.groupBy("Gender").count()
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val schemaText = spark.sparkContext.wholeTextFiles("./src/main/resources/outputSchema.avsc").collect()(0)._2
    //println(schemaText)
    val df_4 = spark.read.option("avroSchema", schemaText).avro("test_data/sample1").toDF()
    val df_212_1 = spark.read.option("avroSchema", schemaText).avro("test_data/sample2").toDF()
    val df_212_2 = spark.read.option("avroSchema", schemaText).avro("test_data/sample3").toDF()
    val df_316 = spark.read.option("avroSchema", schemaText).avro("test_data/sample4").toDF()

    /*
    Taking union of df_212_1 and df_212_2, because there are no overlapping Identifiers.
    */
    val df_212 = df_212_1.union(df_212_2)

    /*
    generatePriority function is implemented in PriorityGenerator.scala File.
     */
    val priority = generatePriority(List(4, 212, 316))

    /*
    mergeDF function is implemented in MergeDF.scala File.
     */
    val mergedDF = mergeDF(df_4, df_212, df_316, priority)

    /*
    Apply age_bucket udf to convert Age column of merged dataframe.
     */
    val finalDF = mergedDF.withColumn("Age", age_bucket(col("Age")))
  }

  def age_bucket: UserDefinedFunction = udf((age: Int) =>
    if (age <= 18) 18
    else if (age > 18 && age <= 25) 25
    else if (age > 25 && age <= 35) 35
    else if (age > 35 && age <= 45) 45
    else if (age > 45 && age <= 55) 55
    else if (age > 55 && age <= 65) 65
    else 75
  )
}
