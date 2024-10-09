package org.connection

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object DataEntry extends App {

  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("SparkPostgresExample")
    .config("spark.master", "local")
    .getOrCreate()

  // Define the schema for the "workitem" table
  val workitemSchema = StructType(Seq(
    StructField("id", StringType, false),
    StructField("projectid", StringType, false),
    StructField("tenantid", StringType, false),
    StructField("workitemid", StringType, false),
    StructField("parentid", StringType), // Change StringType to StringType if parentid is UUID
    StructField("budget_type", StringType), // Change StringType to StringType if budget_type is UUID
    StructField("story_point", DoubleType),
    StructField("progress", IntegerType),
    StructField("name", StringType),
    StructField("description", StringType),
    StructField("start_date", TimestampType), // Explicitly set the data type to TimestampType
    StructField("due_date", TimestampType), // Explicitly set the data type to TimestampType
    StructField("createdby", StringType, false),
    StructField("createddate", TimestampType), // Explicitly set the data type to TimestampType
    StructField("updatedby", StringType),
    StructField("updateddate", TimestampType), // Explicitly set the data type to TimestampType
    StructField("prioritiesid", StringType), // Change StringType to StringType if prioritiesid is UUID
    StructField("sectionid", StringType), // Change StringType to StringType if sectionid is UUID
    StructField("hours", DoubleType),
    StructField("workitemtypeid", StringType), // Change StringType to StringType if workitemtypeid is UUID
    StructField("workitemstatusid", StringType), // Change StringType to StringType if workitemstatusid is UUID
    StructField("workflowid", StringType), // Change StringType to StringType if workflowid is UUID
    StructField("stage", StringType),
    StructField("display_order", IntegerType),
    StructField("overdue", BooleanType, false),
    StructField("logged_time", DoubleType),
    StructField("subworkitem_logged_time", DoubleType),
    StructField("time_entry_count", IntegerType),
    StructField("subworkitem_time_entry_count", IntegerType),
    StructField("status", StringType),
    StructField("customfields", MapType(StringType, StringType)) // Assuming customfields is a Map of String keys and String values
  ))

  val data = Seq(
    Row("1", "project1", "tenant1", "workitem1", null, "BudgetA", 3.5, 75, "WorkItem 1", "Description 1", "2023-09-10 12:45:00", "2023-09-15 14:30:00", "user1", "2023-09-10 12:45:00", null, "prioritiesid1", "sectionid1", 0.0, "workitemtypeid1", "workitemstatusid1", "workflowid1", "StageA", 1, false, 2.5, 1.0, 2, 0, "Active", Map("customField1" -> "Value1", "customField2" -> "Value2")),
  )
  val workitemDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), workitemSchema)
  workitemDF.show()


  // Define the column names based on your schema
  val columns = Seq(
    "id", "projectid", "tenantid", "workitemid", "parentid", "budget_type",
    "story_point", "progress", "name", "description", "start_date", "due_date",
    "createdby", "createddate", "updatedby", "updateddate", "prioritiesid",
    "sectionid", "hours", "workitemtypeid", "workitemstatusid", "workflowid",
    "stage", "display_order", "overdue", "logged_time", "subworkitem_logged_time",
    "time_entry_count", "subworkitem_time_entry_count", "status", "customfields"
  )

  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "admin")
  connectionProperties.setProperty("password", "VMKSfewtWzad")

  // Write the DataFrame to PostgreSQL
  workitemDF.write
    .mode(SaveMode.Append) // You can use SaveMode.Overwrite or SaveMode.Ignore as needed
    .jdbc(jdbcUrl, "public.workitem", connectionProperties)

  // Stop the SparkSession
  spark.stop()
}

