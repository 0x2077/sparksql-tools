package my.example.domain

import java.io.{File, PrintWriter}
//import org.apache.spark.sql.types.DataTypes._
//import org.apache.spark.sql.types.DecimalType
import scala.io.Source

case class Column(name: String, typeName: String)

object ArraySplitter {
  def covertType(sqlType: String): Object = {
    def extractTypeName(input: String) = {
      "[a-z_]+".r.findFirstIn(input)
    }

    extractTypeName(sqlType.trim.toLowerCase) match {
      // scalastyle:off
      case Some("date")                    => "DateType"
      case Some("time")                    => "TimestampType"
      case Some("time_with_timezone")      => "TimestampType"
      case Some("timestamp")               => "TimestampType"
      case Some("timestamp_with_timezone") => "TimestampType"
      case Some("boolean")                 => "BooleanType"
      case Some("tinyint")                 => "IntegerType"
      case Some("smallint")                => "IntegerType"
      case Some("int")                     => "IntegerType"
      case Some("integer")                 => "IntegerType"
      case Some("bigint")                  => "DecimalType"
      case Some("numeric")                 => "DecimalType"
      case Some("decimal")                 => "DecimalType"
      case Some("float")                   => "FloatType"
      case Some("real")                    => "DoubleType"
      case Some("double")                  => "DoubleType"
      case Some("char")                    => "StringType"
      case Some("nchar")                   => "StringType"
      case Some("varchar")                 => "StringType"
      case Some("nvarchar")                => "StringType"
      case Some("binary")                  => "BinaryType"
      case Some("varbinary")               => "BinaryType"
      // scalastyle:on
    }
  }

  def readColumnFile(name: String): List[Column] = {
    val source = Source.fromFile(name)
    val fileContents = source.getLines.toList
    source.close

    fileContents.map {
      line =>
        val keyValuePairs = line
          .replaceAll("\\s+", ":") // replace N whitespaces with ":"
          .split(":")

        Column(keyValuePairs(0), keyValuePairs(1))
    }
  }

  def processColumns(columns: List[Column], dfName: String, arrayFieldName: String): String = {
    columns
      .zipWithIndex
      .map {
        case (column, idx) =>
          s"""$dfName.withColumn("${column.name}", $dfName("$arrayFieldName")($idx).cast(${covertType(column.typeName)})"""
      }
      .mkString("\n")
  }

  def writeResult(dir: String, fileName: String, outputFileSuffix: String, data: String) = {
    val pw = new PrintWriter(new File(s"$dir/$fileName$outputFileSuffix"))
    pw.write(data)
    pw.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val dir = new File(conf.dir.toOption.get)
    val dfName = conf.inputDfName.toOption.get
    val arrayFieldName = conf.inputDfArrayColumnName.toOption.get
    val inputFileSuffix = conf.inputFileSuffix.toOption.get
    val outputFileSuffix = conf.outputFileSuffix.toOption.get

    val inputFiles = dir
      .listFiles
      .filter(_.getName.toLowerCase.endsWith(inputFileSuffix))
      .toList

    inputFiles
      .foreach { file =>
        val cols = readColumnFile(file.getPath)
        val processedCols: String = processColumns(cols, dfName, arrayFieldName)
        writeResult(file.getParent, file.getName, outputFileSuffix, processedCols)
      }
  }

}
