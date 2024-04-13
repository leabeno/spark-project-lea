package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame

trait Reader {
  def read(path: String, format: String, options: Map[String, String]): DataFrame
  def readParquet(path: String): DataFrame
}


