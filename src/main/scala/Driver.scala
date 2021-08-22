import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import MergeDF.mergeDF
import PriorityGenerator.generatePriority
object Driver {

    /*
    Return the count of the overlapping rows in 2 dataframes using "Identifier" column.
     */
    def countOverlap(df1:DataFrame,df2:DataFrame):Long = {
        df1.join(df2,"Identifier").count()
    }


    /*
    Returns the count of rows in a dataframe for a particular DpId.
     */
    def getCount(df:DataFrame,DpId:Int):Any = {
        val res = df.groupBy("DpId")
          .count()
          .filter(x=>x(0) == DpId)
          .head
        res(1)
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

        val priority = generatePriority(List(4,212,316))

        val mergedDF = mergeDF(df_4, df_212, df_316, priority)
        mergedDF.groupBy("DpId").count().show()
    }
}
