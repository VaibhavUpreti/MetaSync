import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object StreamToDW {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StreamToDW")
      .getOrCreate()

    import spark.implicits._

    val salesStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("subscribePattern", "postgres.public.*")
      .load()

    val salesData = salesStream.selectExpr("CAST(value AS STRING)").as[String]

    val salesDF = salesData.select(from_json($"value", schema).as("data"))
      .select("data.*")

    val aggregatedSales = salesDF.groupBy("productID").agg(sum("amount").as("totalSales"))

    // Write aggregated data to your PostgreSQL database
    val query = aggregatedSales.writeStream
      .outputMode("append")
      .format("jdbc")
      .option("url", "jdbc:postgresql://host.docker.internal:5432/metasync_development") 
      .option("dbtable", "users")
      .option("user", "postgres")
      .option("password", "postgres")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    query.awaitTermination()
  }
}

