package org.connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

object Common {

  val retryIntervalInMillis = 5000 // 5 seconds
  val minBackOffInMillis = 1000 // 1 second
  val maxBackOffInMillis = 60000 // 60 seconds

  val properties = new java.util.Properties()
  properties.load(getClass.getResourceAsStream("/database.properties"))

  val config = new java.util.Properties()
  config.load(getClass.getResourceAsStream("/config.properties"))
  val configTimestamp = config.getProperty("timestamp")
  //println(configTimestamp)

  val jdbcUrl = properties.getProperty("database.url")
  val dbUser = properties.getProperty("database.user")
  val dbPassword = properties.getProperty("database.password")

  val sparkConf = new SparkConf()
    .setAppName("connection from postgre")
    .set("spark.authenticate", "true")
    .set("spark.authenticate.secret", "12345")
    .set("spark.ui.reverseProxy", "true")
    .set("spark.master", "local")
    .set("spark.task.maxFailures", "3")
    .set("spark.speculation.retryInterval", retryIntervalInMillis.toString)
    .set("spark.speculation.minExponentialBackOff", minBackOffInMillis.toString)
    .set("spark.speculation.maxExponentialBackOff", maxBackOffInMillis.toString)
    .set("spark.database.url", jdbcUrl) // Set your database URL here
    .set("spark.database.user", dbUser) // Set your database user here
    .set("spark.database.password", dbPassword) // Set your database password here



  val jdbcUrlFromSparkConf = sparkConf.get("spark.database.url")
  val dbUserFromSparkConf = sparkConf.get("spark.database.user")
  val dbPasswordFromSparkConf = sparkConf.get("spark.database.password")

  def getCommonOptions: Map[String, String] = {
    Map("timeout" -> "300", "driver" -> "org.postgresql.Driver",
      "url" -> jdbcUrlFromSparkConf, //spark.conf.get("spark.database.url"),// jdbcUrl,
      "user" -> dbUserFromSparkConf, //spark.conf.get("spark.database.user"), //dbUser,
      "password" -> dbPasswordFromSparkConf //spark.conf.get("spark.database.password")//dbPasswordFromSparkConf //dbPassword
    )
  }


  val sparkSession = SparkSession.builder().appName("connection from postgre")
    .config(sparkConf)
    .getOrCreate()

  def postgresConnection(operation: String): DataFrameReader = operation match {
    case read => sparkSession.read.format("jdbc").options(getCommonOptions)
    case other => throw new IllegalArgumentException(s"Unsupported operation: $other")
  }

  def postgresRead(table: String) =
    postgresConnection("read")
      .option("dbtable", s"${table}")
      .load()

  def postgresReadRetries(table: String)(maxAttemp: Int): DataFrame = {
    require(maxAttemp > 0, "maxAttemp must be greater than 0")
    Try(postgresRead(table: String)) match {
      case Success(value) => value
      case Failure(exception) =>
        if (maxAttemp == 1) throw exception
        postgresReadRetries(table: String)(maxAttemp - 1)
    }
  }

  def writeToPostgres(outputDataframee: DataFrame, outputTable: String): Unit =
    outputDataframee.write.format("jdbc")
      .mode(SaveMode.Overwrite)
      .options(getCommonOptions)
      .option("dbtable", outputTable)
      .saveAsTable("outputTable")

  def validateDateFormat(dateStr: String): Try[Unit] = Try {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.setLenient(false)
    dateFormat.parse(dateStr)
  }

  def writeDataToDb(inputTable: String, outputTable: String): Unit = {
    val finalDF2 = postgresRead(inputTable)
    finalDF2.show()
    val outputDataframe = readLastHourWorkitem("public.workitem2",configTimestamp)//finalDF2.where(col("id") === 26) //write custom code for transformation
    val outputDF = writeToPostgres(outputDataframe, outputTable)
  }


  def writeDataToDbAppendOverwrite(inputTable: String, outputTable: String): Unit = {
    val finalDF2 = postgresRead(inputTable)
    finalDF2.show()
    val outputDataframe = finalDF2.where(col("id") === 28) //write custom code for transformation
    val outputDF = writeToPostgresApendAndOverwrite(outputDataframe, outputTable)
  }

  def writeToPostgresApendAndOverwrite(outputDataframe: DataFrame, outputTable: String): Unit = {
    // Read the existing data from the table
    sparkSession.sparkContext.setLogLevel("ERROR")
    val existingData = postgresRead(outputTable)
    println("-----------------existing data------------------------------")
    existingData.show()
    val uniqueOutputDF = outputDataframe.dropDuplicates()

    // Union the existing data with the new data
    val combinedData = existingData.union(uniqueOutputDF)
    println("---------------------combined data-----------------------------------------------")
    combinedData.show()

    println("---------------------combined data newwwww-----------------------------------------------")
    val uniqueCombinedData = combinedData.dropDuplicates().coalesce(1)
    uniqueCombinedData.show

    writeToPostgres(uniqueCombinedData, outputTable)

    postgresRead(outputTable).show()
  }


  def postgresReadWithFilteredData(table: String, date_column_name: String, dateRequired: String): Dataset[Row] = {
    val dateValidationResult = validateDateFormat(dateRequired)

    dateValidationResult match {
      case Success(_) =>
        val postgresReadDf = postgresConnection("read")
          .option("dbtable", s"${table}").load()

        val dfWithDateAndTime = postgresReadDf.withColumn("date_column", date_format(col(date_column_name), "yyyy-MM-dd"))
          .withColumn("time_column", date_format(col(date_column_name), "HH:mm:ss"))
        dfWithDateAndTime.show()
        //dfWithDateAndTime.count()
        val b = dfWithDateAndTime.count()
        println("original count" + b)

        val targetDate = dateRequired
        val filteredDF = dfWithDateAndTime.filter(to_date(col("date_column")) === targetDate)

        filteredDF.show()
        val c = filteredDF.count()
        println("filtered count" + c)

        filteredDF

      case Failure(exception) =>
        throw new IllegalArgumentException(s"Invalid date format: ${exception.getMessage}. Date must be in yyyy-MM-dd format.")
    }

  }



  def postgresReadWithStartAndEndDate(table: String, date_column_name: String, startDate: String, endDate: String) = {
    val (start,end) = (validateDateFormat(startDate),validateDateFormat(endDate))

    (start,end) match {
      case (Success(_),Success(_)) =>
        val postgresReadDf = postgresConnection("read")
          .option("dbtable", s"${table}").load()//use filter to get updated value greater than our set value

        val dfWithDateAndTime = postgresReadDf.withColumn("date_column", date_format(col(date_column_name), "yyyy-MM-dd"))
          .withColumn("time_column", date_format(col(date_column_name), "HH:mm:ss"))
        dfWithDateAndTime.show()
        //dfWithDateAndTime.count()
        val b = dfWithDateAndTime.count()
        println("original count" + b)

        val filteredDF = dfWithDateAndTime.filter(col("date_column").between(startDate, endDate))

        filteredDF.show()
        val c = filteredDF.count()
        println("filtered count" + c)

        filteredDF

        filteredDF.select(col("date_column"),col("time_column"),col("status"),col("updateddate"))
          .groupBy(col("status"),col("date_column"))
          .count().show()//count()

      case (Failure(_),Failure(_)) | (Success(_),Failure(_)) | (Failure(_),Success(_))=>
        throw new IllegalArgumentException(s"Invalid date format:  Date must be in yyyy-MM-dd format.")
    }

  }

  def postgresReadWithCustomKpi(table: String, date_column_name: String, startDate: String, endDate: String) = {
    val (start, end) = (validateDateFormat(startDate), validateDateFormat(endDate))

    (start, end) match {
      case (Success(_), Success(_)) =>
        val postgresReadDf = postgresConnection("read")
          .option("dbtable", s"${table}").load() //use filter to get updated value greater than our set value

        val dfWithDateAndTime = postgresReadDf.withColumn("date_column", date_format(col(date_column_name), "yyyy-MM-dd"))
          .withColumn("time_column", date_format(col(date_column_name), "HH:mm:ss"))
        dfWithDateAndTime.show()
        //dfWithDateAndTime.count()
        val b = dfWithDateAndTime.count()
        println("original count" + b)

        val filteredDF = dfWithDateAndTime.filter(col("date_column").between(startDate, endDate))

        filteredDF.show()
        val c = filteredDF.count()
        println("filtered count" + c)

        filteredDF

        filteredDF.select(col("date_column"), col("time_column"), col("status"), col("updateddate"),col("story_point"))
          .where(col("status")==="In-Progress")
          .groupBy(col("status"))//.sum(col("story_point"))
          .agg(
            sum(col("story_point")),
            count("*")
          )
          .show() //count()

      case (Failure(_), Failure(_)) | (Success(_), Failure(_)) | (Failure(_), Success(_)) =>
        throw new IllegalArgumentException(s"Invalid date format:  Date must be in yyyy-MM-dd format.")
    }

  }

  def readLastHourWorkitem(table: String, timestamp: String):DataFrame = {

    val postgresReadDf = postgresConnection("read")
      .option("dbtable", s"${table}").load()

    val startTime = LocalDateTime.parse(timestamp).withSecond(0)
    val endTime = startTime.withMinute(59).withSecond(59)

   val dfWithTimestamp = postgresReadDf.filter(postgresReadDf("updateddate").geq(lit(startTime.toString))
     .and(postgresReadDf("updateddate").leq(lit(endTime.toString))))

    dfWithTimestamp//.show()

    val filteredDF = dfWithTimestamp
      .withColumn("date_column", date_format(col("updateddate"), "yyyy-MM-dd"))
      .withColumn("time_column", date_format(col("updateddate"), "HH:mm:ss"))

    filteredDF//.show()

    val estimatedVsTotalWorkingHours = filteredDF
      .select(col("date_column"),
      col("time_column"), col("status"), col("updateddate"),
      col("story_point"), col("hours"))
      //.where(col("status") === "In-Progress")
      .groupBy(col("status"),col("time_column"),col("date_column")) //.sum(col("story_point"))
      .agg(
        sum(col("story_point")),
        count("*").as ("status_count"),
        sum(col("hours")) as("estimated_time")
      )

    estimatedVsTotalWorkingHours

  }


  def automatedJoin(leftDF: DataFrame, rightDF: DataFrame, condition: Column): DataFrame = {
    leftDF.join(rightDF, condition, "inner")
  }

}
