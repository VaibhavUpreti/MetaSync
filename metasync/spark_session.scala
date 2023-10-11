import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object StreamToDW {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StreamToDW")
      .getOrCreate()

    import spark.implicits._

    // Read from Kafka topic
    val salesStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sales_stream")
      .load()

    // Assume the value is a JSON string
    val salesData = salesStream.selectExpr("CAST(value AS STRING)").as[String]

    // Convert JSON to DataFrame
    val salesDF = salesData.select(from_json($"value", schema).as("data"))
      .select("data.*")

    // Aggregate (for example, total sales by product)
    val aggregatedSales = salesDF.groupBy("productID").agg(sum("amount").as("totalSales"))

    // Write aggregated data to your PostgreSQL database
    val query = aggregatedSales.writeStream
      .outputMode("append")
      .format("jdbc")
      .option("url", "jdbc:postgresql://your_db_host:5432/db_name") 
      .option("dbtable", "your_dw_table")
      .option("user", "your_username")
      .option("password", "your_password")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    query.awaitTermination()
  }
}

