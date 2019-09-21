package sparkclass

import org.apache.spark.sql.types._
import fastparse._, MultiLineWhitespace._

object PrintSchema {

  def int[_: P]: P[Int] =
    P(CharIn("0-9").rep(1).!.map(_.toInt))

  def boolean[_: P]: P[Boolean] = P(
    P("true").map(_ => true) |
      P("false").map(_ => false)
  )

  def sparkType[_: P]: P[DataType] = P(
    P("byte").map(_ => ByteType) |
      P("short").map(_ => ShortType) |
      P("integer").map(_ => IntegerType) |
      P("long").map(_ => LongType) |
      P("float").map(_ => FloatType) |
      P("double").map(_ => DoubleType) |
      P("decimal(" ~ int ~ "," ~ int ~ ")").map {
        case (p, s) => DecimalType(p, s)
      } |
      P("boolean").map(_ => BooleanType) |
      P("string").map(_ => StringType) |
      P("date").map(_ => DateType) |
      P("timestamp").map(_ => TimestampType)
  )

  def structField[_: P]: P[StructField] =
    P("|--" ~ CharsWhile(_ != ':').! ~ ":" ~ sparkType ~ "(nullable" ~ "=" ~ boolean ~ ")")
      .map {
        case (name, dataType, nullable) => StructField(name, dataType, nullable)
      }

  def structType[_: P]: P[StructType] =
    P("root" ~ structField.rep ~ End).map { fields =>
      StructType(fields.toArray)
    }

  def parse(printSchema: String): Parsed[StructType] =
    fastparse.parse(printSchema, structType(_))
}
