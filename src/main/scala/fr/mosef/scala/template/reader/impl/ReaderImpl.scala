package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader
import org.apache.spark

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }
  def read(path: String, sep: String = ","): DataFrame = {
    sparkSession
      .read
      .option("sep", sep)
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
  }

  def readParquet(path: String): DataFrame = {
    sparkSession.read.format("parquet").load(path)
  }

  override def read(path: String, format: String, options: Map[String, String]): DataFrame = {
    sparkSession.read.options(options).format(format).load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }

}
