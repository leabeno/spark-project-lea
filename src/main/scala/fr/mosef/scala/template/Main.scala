package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer

object Main extends App with Job {
  val cliArgs = args
  val MASTER_URL: String = cliArgs.lift(0).getOrElse("local[1]")
  val SRC_PATH: String = cliArgs.lift(1).getOrElse {
    println("No input defined")
    sys.exit(1)
  }
  val DST_PATH: String = cliArgs.lift(2).getOrElse("./default/output-writer")
  val OUTPUT_FORMAT: String = cliArgs.lift(3).getOrElse("csv")

  override val src_path: String = SRC_PATH
  override val dst_path: String = DST_PATH

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .enableHiveSupport()
    .getOrCreate()

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer()

  val inputDF = SRC_PATH.split("\\.").lastOption match {
    case Some("csv") => reader.read(SRC_PATH, "csv", Map("header" -> "true", "inferSchema" -> "true"))
    case Some("parquet") => reader.readParquet(SRC_PATH)
    case _ => throw new IllegalArgumentException("Unsupported file format")
  }

  val processedDF = processor.process(inputDF)
  writer.write(processedDF, OUTPUT_FORMAT, dst_path)

  sparkSession.stop()
}