import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.BeanProperty.Std
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object PostgresRetryExample {
  def main(args: Array[String]): Unit = {
    // Initialize a SparkSession
    val spark = SparkSession.builder()
      .appName("PostgresRetryExample")
      .getOrCreate()

    // Set the maximum number of retries for Spark tasks
    val maxRetries = 3
    spark.conf.set("spark.task.maxFailures", maxRetries.toString)

    // Set the retryable exceptions (if needed)
    // For a PostgreSQL connection, you might not need to specify additional exceptions
    // If you have custom exceptions, you can add them here
    // val additionalRetryableExceptions = Seq(classOf[MyCustomException].getName)
    // spark.conf.set("spark.speculation.exceptionsWithExponentialBackOff", additionalRetryableExceptions.mkString(","))

    // Set the retry interval between task retries (in milliseconds)
    val retryIntervalInMillis = 5000 // 5 seconds
    spark.conf.set("spark.speculation.retryInterval", retryIntervalInMillis.toString)

    // Set the min and max exponential backoff intervals (in milliseconds)
    val minBackOffInMillis = 1000 // 1 second
    val maxBackOffInMillis = 60000 // 60 seconds
    spark.conf.set("spark.speculation.minExponentialBackOff", minBackOffInMillis.toString)
    spark.conf.set("spark.speculation.maxExponentialBackOff", maxBackOffInMillis.toString)

   println("\nenter Y or N")
    StdIn.readLine().toLowerCase() match {
      case "y" => true
      case other => throw new NoSuchFieldException("Invalid input")
    }

    // Perform your data processing here

    // Stop the SparkSession
    spark.stop()
  }
}
