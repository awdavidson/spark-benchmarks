package com.awd.spark.tpcds

import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit, min, max, percentile_approx}

import scala.util.Try

object BenchmarkSQL {
  def main(args: Array[String]) {
    val tpcdsDataDir = args(0)
    val resultLocation = args(1)
    val dsdgenDir = args(2)
    val format = Try(args(3).toString).getOrElse("parquet")
    val scaleFactor = Try(args(4).toString).getOrElse("1")
    val iterations = args(5).toInt
    val filterQueries = Try(args(6).toString).getOrElse("")
    val logLevelWarn = Try(args(7).toBoolean).getOrElse(false)

    val timeout = 24*60*60

    println(s"DATA DIR is $tpcdsDataDir")

    val spark = SparkSession
      .builder
      .appName(s"TPCDS SQL Benchmark $scaleFactor GB")
      .getOrCreate()

    if (logLevelWarn) {
      spark.sparkContext.setLogLevel("WARN")
    }

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false,
      useStringForDate = false)

    tables.createTemporaryTables(tpcdsDataDir, format)

    val tpcds = new TPCDS(spark.sqlContext)

    var queryFilter: Seq[String] = Seq()

    if (filterQueries.nonEmpty) {
      println(s"Running only for queries: $filterQueries")
      queryFilter = filterQueries.split(",").toSeq
    }

    val filteredQueries = queryFilter match {
      case Seq() => tpcds.tpcds2_4Queries
      case _ => tpcds.tpcds2_4Queries.filter(q => queryFilter.contains(q.name))
    }

    val experiment = tpcds.runExperiment(
      filteredQueries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)

    experiment.waitForFinish(timeout)

    // Collect general results
    val resultPath = experiment.resultPath
    println(s"Reading results at $resultPath")
    val specificResultTable = spark.read.json(resultPath)
    specificResultTable.show()

    // Summary of Results
    val result = specificResultTable
      .withColumn("result", explode(col("results")))
      .withColumn("executionSeconds", col("result.executionTime")/1000)
      .withColumn("queryName", col("result.name"))
    
    result.select("iteration", "queryName", "executionSeconds").show()
    println(s"Final results at $resultPath")

    val aggResults = result.groupBy("queryName")
      .agg(
        percentile_approx(col("executionSeconds").cast("long"), lit(0.5), lit(1000000)).as("medianRuntimeSeconds"),
        min(col("executionSeconds").cast("long")).as("minRuntimeSeconds"),
        max(col("executionSeconds").cast("long")).as("maxRuntimeSeconds"))
      .orderBy(col("queryName"))

    aggResults.coalesce(1).write.csv(s"$resultPath/summary.csv")

    aggResults.show(105)

    spark.stop()

  }
}

