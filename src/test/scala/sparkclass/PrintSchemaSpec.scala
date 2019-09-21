package sparkclass

import fastparse.Parsed
import org.apache.spark.sql.types._
import org.scalatest._

class PrintSchemaSpec extends FlatSpec with Matchers {

  val testPrintSchema: String =
    """root
    | |-- byteField: byte (nullable = false)
    | |-- shortField: short (nullable = false)
    | |-- intField: integer (nullable = true)
    | |-- longField: long (nullable = true)
    | |-- floatField: float (nullable = true)
    | |-- doubleField: double (nullable = true)
    | |-- decimalField: decimal(12, 5) (nullable = true)
    | |-- booleanField: boolean (nullable = true)
    | |-- stringField: string (nullable = true)
    | |-- dateField: date (nullable = true)
    | |-- timestampField: timestamp (nullable = true)
    |""".stripMargin

  "PrintSchema.parse(testPrintSchema)" should "parse print schema successfully" in {
    PrintSchema.parse(testPrintSchema) shouldEqual Parsed.Success(
      StructType(
        Array(
          StructField("byteField", ByteType, false),
          StructField("shortField", ShortType, false),
          StructField("intField", IntegerType, true),
          StructField("longField", LongType, true),
          StructField("floatField", FloatType, true),
          StructField("doubleField", DoubleType, true),
          StructField("decimalField", DecimalType(12, 5), true),
          StructField("booleanField", BooleanType, true),
          StructField("stringField", StringType, true),
          StructField("dateField", DateType, true),
          StructField("timestampField", TimestampType, true)
        )
      ),
      479
    )
  }
}
