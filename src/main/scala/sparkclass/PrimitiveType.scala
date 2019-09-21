package sparkclass

import org.apache.spark.sql.types._

sealed trait PrimitiveType

object PrimitiveType {
  final case object Byte extends PrimitiveType
  final case object Short extends PrimitiveType
  final case object Int extends PrimitiveType
  final case object Long extends PrimitiveType
  final case object Float extends PrimitiveType
  final case object Double extends PrimitiveType
  final case class Decimal(precision: Int, scale: Int) extends PrimitiveType
  final case object Boolean extends PrimitiveType
  final case object String extends PrimitiveType
  final case object Date extends PrimitiveType
  final case object Timestamp extends PrimitiveType

  def fromSparkType(sparkType: DataType): PrimitiveType =
    sparkType match {
      case ByteType        => Byte
      case ShortType       => Short
      case IntegerType     => Int
      case LongType        => Long
      case FloatType       => Float
      case DoubleType      => Double
      case dt: DecimalType => Decimal(dt.precision, dt.scale)
      case BooleanType     => Boolean
      case StringType      => String
      case DateType        => Date
      case TimestampType   => Timestamp
      case ty              => throw new IllegalArgumentException(s"Unsupported type: $ty")
    }

  def requiredImport(primitiveType: PrimitiveType): Option[String] =
    primitiveType match {
      case Date      => Some("import java.sql.Date")
      case Timestamp => Some("import java.sql.Timestamp")
      case _         => None
    }
}
