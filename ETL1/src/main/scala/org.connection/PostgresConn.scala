package org.connection

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.connection.Common._
import org.connection.Pipeline.logger
import org.connection.PostgresConn.totalWorkItems
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

object PostgresConn extends App {
  sparkSession.sparkContext.setLogLevel("ERROR")
  private val logger = LoggerFactory.getLogger(getClass)

  lazy  val finalDF = postgresRead("public.employee_demo")

  val timestampColumn = "timestamp"

  logger.info("\n\nconnection crated succesfully\n\n")
  val currentHour = current_timestamp()

  logger.info("*******\n********\n********")
//  finalDF.select(col("id")).show
  val startOfLatestDate = current_date()

  logger.info("\n\nFiltering records with the latest hour\n\n")
  val startOfLatestHour = date_sub(currentHour, 3600)
lazy  val finalDF2 = postgresRead("public.manager_demo")
  //val latestHourRecords = finalDF2.filter(col(timestampColumn) >= startOfLatestDate && col(timestampColumn) <= currentHour)

  val df1 = postgresRead("public.employee_demo")
  //latestHourRecords.show()
  val df2 = postgresRead("public.manager_demo")
  val df3 = postgresRead("public.employeeOutput_demo1")
  val df4 = postgresRead("public.table4")
  val df5 = postgresRead("public.table5")
  val df6 = postgresRead("public.table6")
  val dataFrames = List(df2, df3, df4, df6)


  val join1 = df2.join(df4, df2.col("id") === df4.col("id"), "inner").drop(df4.col("id"))

  val manualJoin2 = join1.join(df6, join1.col("id") === df6.col("rank"), "inner")


  //df1.show()
  //df2.show()
  //df3.show()
  //df4.show()
  //df5.show()
  //df6.show()
  val initialDF = df2

  val runautomatedJoin = dataFrames.tail.foldLeft(initialDF) { (accDF, currentDf) =>
    if (currentDf == df5) {
      val condition = accDF.col("id") === currentDf.col("rank")
      automatedJoin(accDF, currentDf, condition).drop(currentDf.col("rank"))
    }
    if (currentDf == df6) {
      val condition = accDF.col("rank") === currentDf.col("rank")
      automatedJoin(accDF, currentDf, condition).drop(currentDf.col("rank"))
    } else {
      val condition = accDF.col("id") === currentDf.col("id")
      automatedJoin(accDF, currentDf, condition).drop(currentDf.col("id"))
    }
  }
  val df7 = postgresRead("public.workitem")
  logger.info("\n\n-------------this is manualJoin2--testing----------------\n\n")
  manualJoin2.show()
  val df8 = postgresRead("public.tenants")

  val dfWithDateAndTime = df2
    .withColumn("date_column", date_format(col("timestamp"), "yyyy-MM-dd"))
    .withColumn("time_column", date_format(col("timestamp"), "HH:mm:ss"))

  lazy val filteredData = postgresReadWithFilteredData("public.manager_demo", "timestamp", "2023-09-14")

  logger.info("\n\n--------------this is automated join")
  runautomatedJoin.show()

  logger.info("\n\n------------workitem table------------------------------------------\n\n")

  val workitem2Df = postgresRead("public.workitem2")

  workitem2Df.createOrReplaceTempView("workitem2")
  val workitem2Sql = sparkSession.sql("""select * from workitem2""")
  workitem2Sql.show()

  def kPI_readAndWrite = {
    val workitem2SqlWithStatusInProgress = sparkSession.sql("""select createddate,name,description,status,count(*) from workitem2 where status = "In-Progress" group by createddate,name,description,status""")
    workitem2SqlWithStatusInProgress.show()
    writeToPostgres(workitem2SqlWithStatusInProgress, "public.employeeOutput_demo8")
    val demoDf = postgresRead("public.employeeOutput_demo8")
    demoDf.show()
  }

  def kPI_ReadWithFilteredData:DataFrame = {
    postgresReadWithFilteredData("public.workitem2", "updateddate", "2023-10-23")
  }

  def kPI_ReadWithStartAndEndDate: Unit = {
    postgresReadWithStartAndEndDate("public.workitem2", "updateddate", "2023-09-14", "2023-10-23")
  }

  def kPI_CustomKpi: Unit = {
    postgresReadWithCustomKpi("public.workitem2", "updateddate", "2023-09-14", "2023-10-23")
  }

  def kPI_readLastHourWorkitemViaConfig :DataFrame = {
    readLastHourWorkitem("public.workitem2", configTimestamp)
  }

  def kPI_1_Read(table: String): DataFrame = {
    postgresConnection("read")
      .option("dbtable", s"${table}").load()
  }

  def kPI_1_Calculate(timestamp: String, kPI_Read:DataFrame): DataFrame = {

    val startTime = LocalDateTime.parse(timestamp).withSecond(0)

    val endTime = startTime.withMinute(59).withSecond(59)

    val dfWithTimestamp = kPI_Read.filter(kPI_Read("updateddate").geq(lit(startTime.toString))
      .and(kPI_Read("updateddate").leq(lit(endTime.toString))))

    val filteredDF = dfWithTimestamp
      .withColumn("date_column", date_format(col("updateddate"), "yyyy-MM-dd"))
      .withColumn("time_column", date_format(col("updateddate"), "HH:mm:ss"))

    val estimatedVsTotalWorkingHours = filteredDF
      .select(col("date_column"),
        col("time_column"), col("status"), col("updateddate"),
        col("story_point"), col("hours"))
      .groupBy(col("status"), col("time_column"), col("date_column")) //.sum(col("story_point"))
      .agg(
        sum(col("story_point")),
        count("*").as("status_count"),
        sum(col("hours")) as ("estimated_time")
      )

    estimatedVsTotalWorkingHours
  }

  def kPI_1_CalculateRealData(timestamp: String, kPI_Read: DataFrame): DataFrame = {

    // Define a pattern to match your timestamp format
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"

    // Parse the timestamp string
    val originalTimestamp = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern(pattern))

    // Convert to Timestamp
    val timestampAsTimestamp = Timestamp.valueOf(originalTimestamp)

    val startTime = timestampAsTimestamp
    val endTimeupdated = originalTimestamp.plusHours(1)
    val endTime = Timestamp.valueOf(endTimeupdated)

    val dfWithTimestamp = kPI_Read.filter(kPI_Read("updateddate").geq(lit(startTime))
      .and(kPI_Read("updateddate").leq(lit(endTime.toString))))

    val filteredDF = dfWithTimestamp
      .withColumn("date_column", date_format(col("updateddate"), "yyyy-MM-dd"))
      .withColumn("time_column", date_format(col("updateddate"), "HH:mm:ss"))

    val estimatedVsTotalWorkingHours = filteredDF
      .select(col("date_column"),
        col("time_column"), col("status"), col("updateddate"),
        col("story_point"), col("hours"))
      .groupBy(col("status"), col("time_column"), col("date_column")) //.sum(col("story_point"))
      .agg(
        sum(col("story_point")),
        count("*").as("status_count"),
        sum(col("hours")) as ("estimated_time")
      )

    estimatedVsTotalWorkingHours
  }

  lazy val readWorkitemSummary = kPI_1_Read("public.workitem_summary")
  //.cache()
  //.persist(StorageLevel.MEMORY_AND_DISK_SER)

  def completedWorkitems = {
    val completedWorkitems = readWorkitemSummary
      .where(col("stage") === "Done")
      .count()
    val logger = LoggerFactory.getLogger(getClass)
    logger.info(s"count of completed workitem is---$completedWorkitems")
  }

  def workitemsByStatus = {
    val workitemsByStatus = readWorkitemSummary
      .select(col("stage"))
      .groupBy(col("stage"))
      .agg(count("*").as("workitemsByStatus"))
      .show()
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("\n\n------workitemsByStatus is succesfully executed----------\n\n")
  }

  def totalWorkitemByAssignee = {
    val totalWorkitemByAssignee = readWorkitemSummary
      .select(col("assignee"))
      .groupBy(col("assignee"))
      .agg(count("*").as("totalWorkitemByAssignee"))
      .show()
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("\n\n------totalWorkitemByAssignee is succesfully executed----------\n\n")
  }

  def openWorkitemByAssignee = {
    val openWorkitemByAssignee = readWorkitemSummary
      .select(col("assignee"))
      .where(col("stage") =!= "Done")
      .groupBy(col("assignee"))
      .agg(count("*").as("openWorkitemByAssignee"))
      .show()
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("\n\n------openWorkitemByAssignee is succesfully executed----------\n\n")
  }

  def totalWorkItems = {
    val logger = LoggerFactory.getLogger(getClass)
    val totalWorkItems = readWorkitemSummary.count()
    logger.info(s"\n\n-----total work items is-- $totalWorkItems-----\n\n")
  }


  logger.info("\n\n--------------stoping the spark session-------------------\n\n")
  lazy val closeSparkSession = sparkSession.stop()

}
