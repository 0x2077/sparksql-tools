package my.example.domain

//import org.apache.spark.sql.types.{"IntegerType", "StringType", "TimestampType"}
import org.scalatest.{FlatSpec, Matchers}

class ArraySplitterTest extends FlatSpec with Matchers {
  "Output file" should "be created" in {
    ArraySplitter.main(Array[String](
      "--dir", "src/test/resources"
    ))
  }

  "Output file (with required suffix)" should "be created" in {
    ArraySplitter.main(Array[String](
      "--dir", "src/test/resources",
      "--input-df-name", "d",
      "--input-df-array-column-name", "val",
      "--input-file-suffix", "sales.columns",
      "--output-file-suffix", ".query"
    ))
  }

  "int*" should "IntegerType" in {
    ArraySplitter.covertType("int")     shouldBe ("IntegerType")
    ArraySplitter.covertType("integer") shouldBe ("IntegerType")
    ArraySplitter.covertType("Int")     shouldBe ("IntegerType")
    ArraySplitter.covertType("Integer") shouldBe ("IntegerType")
    ArraySplitter.covertType("INT")     shouldBe ("IntegerType")
    ArraySplitter.covertType("INTEGER") shouldBe ("IntegerType")
  }

  "time*" should "TimestampType" in {
    ArraySplitter.covertType("time")                    shouldBe ("TimestampType")
    ArraySplitter.covertType("time_with_timezone")      shouldBe ("TimestampType")
    ArraySplitter.covertType("timestamp")               shouldBe ("TimestampType")
    ArraySplitter.covertType("timestamp_with_timezone") shouldBe ("TimestampType")
    ArraySplitter.covertType("Time")                    shouldBe ("TimestampType")
    ArraySplitter.covertType("Time_with_timezone")      shouldBe ("TimestampType")
    ArraySplitter.covertType("Timestamp")               shouldBe ("TimestampType")
    ArraySplitter.covertType("Timestamp_with_timezone") shouldBe ("TimestampType")
    ArraySplitter.covertType("TIME")                    shouldBe ("TimestampType")
    ArraySplitter.covertType("TIME_WITH_TIMEZONE")      shouldBe ("TimestampType")
    ArraySplitter.covertType("TIMESTAMP")               shouldBe ("TimestampType")
    ArraySplitter.covertType("TIMESTAMP_WITH_TIMEZONE") shouldBe ("TimestampType")
  }

  "*char*" should "StringType" in {
    ArraySplitter.covertType("char")         shouldBe ("StringType")
    ArraySplitter.covertType("varchar")      shouldBe ("StringType")
    ArraySplitter.covertType("nchar")        shouldBe ("StringType")
    ArraySplitter.covertType("nvarchar")     shouldBe ("StringType")
    ArraySplitter.covertType("Char")         shouldBe ("StringType")
    ArraySplitter.covertType("Varchar")      shouldBe ("StringType")
    ArraySplitter.covertType("Nchar")        shouldBe ("StringType")
    ArraySplitter.covertType("Nvarchar")     shouldBe ("StringType")
    ArraySplitter.covertType("Char")         shouldBe ("StringType")
    ArraySplitter.covertType("Varchar")      shouldBe ("StringType")
    ArraySplitter.covertType("Nchar")        shouldBe ("StringType")
    ArraySplitter.covertType("Nvarchar")     shouldBe ("StringType")
    ArraySplitter.covertType("char(10)")     shouldBe ("StringType")
    ArraySplitter.covertType("varchar(10)")  shouldBe ("StringType")
    ArraySplitter.covertType("nchar(10)")    shouldBe ("StringType")
    ArraySplitter.covertType("nvarchar(10)") shouldBe ("StringType")
    ArraySplitter.covertType("Char(10)")     shouldBe ("StringType")
    ArraySplitter.covertType("Varchar(10)")  shouldBe ("StringType")
    ArraySplitter.covertType("Nchar(10)")    shouldBe ("StringType")
    ArraySplitter.covertType("Nvarchar(10)") shouldBe ("StringType")
    ArraySplitter.covertType("Char(10)")     shouldBe ("StringType")
    ArraySplitter.covertType("Varchar(10)")  shouldBe ("StringType")
    ArraySplitter.covertType("Nchar(10)")    shouldBe ("StringType")
    ArraySplitter.covertType("Nvarchar(10)") shouldBe ("StringType")
  }

}
