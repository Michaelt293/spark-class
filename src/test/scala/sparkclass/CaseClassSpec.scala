package sparkclass

import org.apache.spark.sql.types._
import net.jcazevedo.moultingyaml._
import org.scalatest._

class CaseClassSpec extends FlatSpec with Matchers {
  import CaseClass.YamlProtocol._

  val testSchema = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("pets", IntegerType, true),
      StructField("time", TimestampType, false),
      StructField("date", DateType, false)
    )
  )

  val testCaseClass =
    CaseClass(
      "Test",
      None,
      None,
      List(
        Field("name", PrimitiveType.String, false, None),
        Field("pets", PrimitiveType.Int, false, None),
        Field("time", PrimitiveType.Timestamp, true, None),
        Field("date", PrimitiveType.Date, true, None)
      )
    )

  val testCaseClassWithPackage =
    CaseClass(
      "TestWithPackage",
      None,
      Some("org.test"),
      List(
        Field("name", PrimitiveType.String, false, Some("Person's name")),
        Field("pets", PrimitiveType.Int, false, Some("Number of pets")),
        Field(
          "time",
          PrimitiveType.Timestamp,
          true,
          Some("Time of last pet purchase")
        ),
        Field("date", PrimitiveType.Date, true, None)
      )
    )

  "CaseClass.fromSchema(\"Test\", schema)" should "" in {
    CaseClass.fromSchema("Test", testSchema) shouldEqual testCaseClass
  }

  "testCaseClass.toYaml.prettyPrint" should "be formatted correctly" in {
    testCaseClass.toYaml.prettyPrint shouldEqual
      """|name: Test
         |fields:
         |- fieldName: name
         |  primitiveType: String
         |  dataDescription: ''
         |- fieldName: pets
         |  primitiveType: Int
         |  dataDescription: ''
         |- fieldName: time
         |  primitiveType: Timestamp
         |  required: true
         |  dataDescription: ''
         |- fieldName: date
         |  primitiveType: Date
         |  required: true
         |  dataDescription: ''
         |""".stripMargin
  }

  "testCaseClassWithPackage.toYaml.prettyPrint" should "be formatted correctly" in {
    testCaseClassWithPackage.toYaml.prettyPrint shouldEqual
      """|name: TestWithPackage
         |packagePath: org.test
         |fields:
         |- fieldName: name
         |  primitiveType: String
         |  dataDescription: Person's name
         |- fieldName: pets
         |  primitiveType: Int
         |  dataDescription: Number of pets
         |- fieldName: time
         |  primitiveType: Timestamp
         |  required: true
         |  dataDescription: Time of last pet purchase
         |- fieldName: date
         |  primitiveType: Date
         |  required: true
         |  dataDescription: ''
         |""".stripMargin
  }

  "testCaseClass.showCaseClass" should "be formatted correctly" in {
    testCaseClass.showCaseClass shouldEqual
      """|case class Test(
         |  name: Option[String],
         |  pets: Option[Int],
         |  time: Timestamp,
         |  date: Date
         |)
         |""".stripMargin
  }

  "testCaseClassWithPackage.showCaseClassWithImports" should "be formatted correctly" in {
    testCaseClassWithPackage.showCaseClassWithImports shouldEqual
      """|import java.sql.Date
         |import java.sql.Timestamp
         |
         |case class TestWithPackage(
         |  name: Option[String],
         |  pets: Option[Int],
         |  time: Timestamp,
         |  date: Date
         |)
         |""".stripMargin
  }

  "testCaseClassWithPackage.showCaseClassAll" should "be formatted correctly" in {
    testCaseClassWithPackage.showCaseClassAll shouldEqual
      """|package org.test
         |
         |import java.sql.Date
         |import java.sql.Timestamp
         |
         |/**
         | * Data model for TestWithPackage
         | *
         | *  @param name Person's name
         | *  @param pets Number of pets
         | *  @param time Time of last pet purchase
         | *  @param date
         | */
         |case class TestWithPackage(
         |  name: Option[String],
         |  pets: Option[Int],
         |  time: Timestamp,
         |  date: Date
         |)
         |""".stripMargin
  }
}
