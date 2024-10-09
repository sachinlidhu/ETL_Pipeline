package org.connection

import org.apache.spark.sql.functions.{approx_count_distinct, col, count, count_distinct}
import org.apache.spark.storage.StorageLevel
import org.connection.Common._
import org.connection.PostgresConn.{getClass, _}
import org.slf4j.LoggerFactory

object Pipeline extends App {

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("\n\n-------------spark session creatttinggggggggg------------------\n\n")

  sparkSession
  val startTime = System.nanoTime()
  readWorkitemSummary
  completedWorkitems
  workitemsByStatus
  totalWorkitemByAssignee
  openWorkitemByAssignee
  totalWorkItems
  val endTime = System.nanoTime()
  val elapsedTimeInSeconds = (endTime - startTime) / 1e9
  logger.info(s"\n\nResponse time: $elapsedTimeInSeconds seconds\n\n")
  closeSparkSession
}


