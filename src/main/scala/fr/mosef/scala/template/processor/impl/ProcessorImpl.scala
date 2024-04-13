package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, to_date, year}


class ProcessorImpl extends Processor {
  def process(inputDF: DataFrame): DataFrame = {
    val dfWithDate = inputDF.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
    val dfWithYear = dfWithDate.withColumn("Year", year(col("Date")))
    dfWithYear.groupBy("Year").agg(avg("Volume").as("AvgVolume"), avg("Adj Close").as("AvgAdjClose")).orderBy("Year")
  }
}

