package sparkclass

import org.apache.spark.sql.types._
import net.jcazevedo.moultingyaml._
import fastparse._, NoWhitespace._

final case class CaseClass(
    name: String,
    tableDescription: Option[String],
    packagePath: Option[String],
    fields: List[Field]
) {
  def showPackage: Option[String] =
    packagePath.map("package " + _)

  def requiredImports: List[String] =
    fields.flatMap(f => PrimitiveType.requiredImport(f.primitiveType))

  def showImports: String =
    if (requiredImports.isEmpty) ""
    else
      requiredImports.mkString(
        "",
        System.lineSeparator(),
        System.lineSeparator() * 2
      )

  def showDocComment: String = {
    val params = fields.map(f =>
      s" *  @param ${f.fieldName}${f.dataDescription.fold("")(" " + _)}")

    params.mkString(
      s"""|/**
          | * Data model for $name${tableDescription.fold("")(description =>
           ": " + description)}
          | *
          |""".stripMargin,
      System.lineSeparator(),
      System.lineSeparator() + " */" + System.lineSeparator()
    )
  }

  def showCaseClass: String =
    s"""|case class ${name}(
        |${fields
         .map(f => s"  ${f.fieldName}: ${f.showPrimitiveType}")
         .mkString(s",${System.lineSeparator()}")}
        |)
        |""".stripMargin

  def showCaseClassWithImports: String =
    showImports + showCaseClass

  def showCaseClassAll: String =
    showPackage.fold("")(_ + System.lineSeparator() * 2) +
      showImports +
      showDocComment +
      showCaseClass
}

object CaseClass {

  def fromSchema(name: String, struct: StructType): CaseClass = {
    val fields: List[Field] =
      struct.fields.toList.map { field =>
        Field(
          field.name,
          PrimitiveType.fromSparkType(field.dataType),
          !field.nullable,
          None
        )
      }

    CaseClass(name, None, None, fields)
  }

  def fromPrintSchema(name: String, printSchema: String): CaseClass =
    PrintSchema.parse(printSchema) match {
      case Parsed.Success(schema, _) => fromSchema(name, schema)
      case err @ Parsed.Failure(_, _, _) =>
        throw new IllegalArgumentException("Parse Error, " + err.msg)
    }

  object YamlProtocol extends DefaultYamlProtocol {
    implicit val primitiveTypeFormat: YamlFormat[PrimitiveType] =
      new YamlFormat[PrimitiveType] {
        import PrintSchema._

        def write(ty: PrimitiveType): YamlString =
          ty match {
            case PrimitiveType.Byte       => YamlString("Byte")
            case PrimitiveType.Short      => YamlString("Short")
            case PrimitiveType.Int        => YamlString("Int")
            case PrimitiveType.Long       => YamlString("Long")
            case PrimitiveType.Float      => YamlString("Float")
            case PrimitiveType.Double     => YamlString("Double")
            case _: PrimitiveType.Decimal => YamlString("Decimal")
            case PrimitiveType.Boolean    => YamlString("Boolean")
            case PrimitiveType.String     => YamlString("String")
            case PrimitiveType.Date       => YamlString("Date")
            case PrimitiveType.Timestamp  => YamlString("Timestamp")
          }

        private def decimalParser[_: P]: P[DecimalType] =
          P("Decimal(" ~ int ~ "," ~ int ~ ")").map {
            case (p, s) => DecimalType(p, s)
          }

        def read(value: YamlValue): PrimitiveType = value match {
          case YamlString("Byte")      => PrimitiveType.Byte
          case YamlString("Short")     => PrimitiveType.Short
          case YamlString("Int")       => PrimitiveType.Int
          case YamlString("Long")      => PrimitiveType.Long
          case YamlString("Float")     => PrimitiveType.Float
          case YamlString("Double")    => PrimitiveType.Double
          case YamlString("Boolean")   => PrimitiveType.Boolean
          case YamlString("String")    => PrimitiveType.String
          case YamlString("Date")      => PrimitiveType.Date
          case YamlString("Timestamp") => PrimitiveType.Timestamp
          case YamlString(decimal) =>
            fastparse.parse(decimal, decimalParser(_)) match {
              case Parsed.Success(dt, _) =>
                PrimitiveType.Decimal(dt.precision, dt.scale)
              case err @ Parsed.Failure(_, _, _) =>
                deserializationError("Parse Error, " + err.msg)
            }
          case ty => deserializationError(s"Unsupported type: $ty")
        }
      }

    implicit val fieldFormat: YamlFormat[Field] = new YamlFormat[Field] {
      def write(f: Field): YamlObject = {
        val elems = f match {
          case Field(fieldName, primitiveType, required, dataDescription) =>
            Seq(
              Some(YamlString("fieldName") -> fieldName.toYaml),
              Some(YamlString("primitiveType") -> primitiveType.toYaml),
              Some(YamlString("required") -> required.toYaml)
                .filter(_._2.convertTo[Boolean]),
              Some(
                YamlString("dataDescription") ->
                  dataDescription.fold(YamlString("")) { data =>
                    YamlString(data.toString)
                  }
              )
            )
        }

        YamlObject(elems.flatten: _*)
      }

      def read(value: YamlValue): Field = value match {
        case YamlObject(fields) =>
          Field(
            selectField("fieldName", fields).convertTo[String],
            selectField("primitiveType", fields).convertTo[PrimitiveType],
            fields
              .get(YamlString("required"))
              .fold(false)(_.convertTo[Boolean]),
            fields
              .get(YamlString("dataDescription"))
              .map(_.convertTo[String])
          )

        case other =>
          deserializationError(
            "YamlObject expected, but got " + other
          )
      }
    }

    private def selectField(
        fieldName: String,
        fields: Map[YamlValue, YamlValue]
    ): YamlValue =
      try fields(YamlString(fieldName))
      catch {
        case e: NoSuchElementException =>
          deserializationError(
            "YamlObject is missing required member '" +
              fieldName + "'",
            e,
            fieldName :: Nil
          )
      }

    implicit val caseClassFormat: YamlFormat[CaseClass] =
      yamlFormat4(CaseClass.apply)
  }
}
