package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

class Writer {
  def write(df: DataFrame, format: String, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .format(format)
      .mode(mode)
      .option("header", "true")
      .save(path)
  }
}
