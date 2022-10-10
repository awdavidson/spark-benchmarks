package com.awd.spark.tpcds

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

import scala.util.Try

object DataGeneration {
  def main(args: Array[String]) {
    val tpcdsDataDir = args(0)
    val dsdgenDir = args(1)
    val format = Try(args(2).toString).getOrElse("parquet")
    val scaleFactor = Try(args(3).toString).getOrElse("1")
    val genPartitions = Try(args(4).toInt).getOrElse(100)
    val partitionTables = Try(args(5).toBoolean).getOrElse(false)
    val clusterByPartitionColumns = Try(args(6).toBoolean).getOrElse(false)
    val logLevelWarn = Try(args(7).toBoolean).getOrElse(false)


    println(s"DATA DIR is $tpcdsDataDir")
    println(s"Scale facor is $scaleFactor GB")

    val spark = SparkSession
      .builder
      .appName(s"TPCDS Generate Data $scaleFactor GB")
      .getOrCreate()

    if (logLevelWarn) {
      spark.sparkContext.setLogLevel("WARN")
    }

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false,
      useStringForDate = false)

    println("Generation TPCDS Data")

    tables.genData(
      location = tpcdsDataDir,
      format = format, 
      overwrite = true,
      partitionTables = partitionTables,
      clusterByPartitionColumns = clusterByPartitionColumns,
      filterOutNullPartitionValues = false,
      tableFilter = "",
      numPartitions = genPartitions)

    println(s"Data generated at $tpcdsDataDir")

    spark.stop()
    
  }

}
