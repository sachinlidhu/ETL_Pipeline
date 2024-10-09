package org.connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.StdIn

object retry extends App {
  // Set the maximum number of retries for Spark tasks in a SparkConf
  val sparkConf = new SparkConf()
    .setAppName("PostgresRetryExample")
    .setMaster("local[*]") // Use "local" or a specific Spark cluster URL here
    .set("spark.task.maxFailures", "3") // Set maxFailures here

  // Initialize a SparkSession with the SparkConf
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

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

  println("\nEnter 'Y' or 'N'")
  StdIn.readLine().toLowerCase() match {
    case "y" =>
    // Perform your data processing here

    case other =>
      println("Invalid input. Exiting the application.")
  }

  // Stop the SparkSession
  spark.stop()
}
