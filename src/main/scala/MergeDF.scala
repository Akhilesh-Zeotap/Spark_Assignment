import Driver.generatePriority
import org.apache.spark.sql.{DataFrame, SparkSession}

object MergeDF extends App{

  def mergeDF(df_4: DataFrame, df_212: DataFrame, df_316: DataFrame, priority: List[Int]): DataFrame = {
    priority.head match {
      case 4 =>
        priority.tail.head match {
          case 212 =>
            df_4.union(df_212).dropDuplicates("Identifier")
              .union(df_316).dropDuplicates("Identifier")
          case 316 =>
            df_4.union(df_316).dropDuplicates("Identifier")
              .union(df_212).dropDuplicates("Identifier")
        }

      case 212 =>
        priority.tail.head match {
          case 4 =>
            df_212.union(df_4).dropDuplicates("Identifier")
              .union(df_316).dropDuplicates("Identifier")
          case 316 =>
            df_212.union(df_316).dropDuplicates("Identifier")
              .union(df_4).dropDuplicates("Identifier")
        }

      case 316 =>
        priority.tail.head match {
          case 4 =>
            df_316.union(df_4).dropDuplicates("Identifier")
              .union(df_212).dropDuplicates("Identifier")

          case 212 =>
            df_316.union(df_212).dropDuplicates("Identifier")
              .union(df_4).dropDuplicates("Identifier")

        }
    }
  }


}
