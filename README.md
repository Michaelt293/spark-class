# `spark class`

### Introduction

`spark class` can be used to derive case class definitions from spark schemas. This should improve the initial development process where case class definitions for input tables are required. In addition, when developing ETL code, it may be preferable to work interactively using Spark's dataset API rather than the dataframe API.

### Getting started

To use `spark class`, first clone this repository and ensure that SBT is installed. `spark class` may be ran interactively either in the Scala REPL or within a spark-shell session.

To run in the the Scala REPL, enter `sbt console` from the project root directory and import a local `SparkSession` (`import sparkclass.LocalSparkSession._`).

To run spark class within a spark-shell session, first assemble a fat jar -

`sbt assembly`

This will build a jar in the following location -

`target/scala-2.11/spark-class-assembly-0.1.0-SNAPSHOT.jar`

`spark-shell` may then be ran with `spark-class-assembly-0.1.0-SNAPSHOT.jar` as follows (you may wish to first upload the jar to a server/cluster with spark installed) -

`spark-shell --jars /path/to/spark-class-assembly-0.1.0-SNAPSHOT.jar`

### Using `spark shell` interactively

Required import -

```
scala> import sparkclass._
import sparkclass._
```

Read a table into a `DataFrame` (for this example, using `export.csv` from https://docs.databricks.com/spark/latest/data-sources/read-csv.html) -

```
scala> val exportDF = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/export.csv")
exportDF: org.apache.spark.sql.DataFrame = [_c0: int, carat: double ... 9 more fields]
```

Get the schema of the `DataFrame` -

```
scala> val exportSchema = exportDF.schema
exportSchema: org.apache.spark.sql.types.StructType = StructType(StructField(_c0,IntegerType,true), StructField(carat,DoubleType,true), StructField(cut,StringType,true), StructField(color,StringType,true), StructField(clarity,StringType,true), StructField(depth,DoubleType,true), StructField(table,DoubleType,true), StructField(price,IntegerType,true), StructField(x,DoubleType,true), StructField(y,DoubleType,true), StructField(z,DoubleType,true))
```

Convert the schema into `spark class`'s `CaseClass` datatype -

```
scala> val caseClass = CaseClass.fromSchema("Export", exportSchema)
caseClass: sparkclass.CaseClass = CaseClass(Export,None,None,List(Field(_c0,Int,false,None), Field(carat,Double,false,None), Field(cut,String,false,None), Field(color,String,false,None), Field(clarity,String,false,None), Field(depth,Double,false,None), Field(table,Double,false,None), Field(price,Int,false,None), Field(x,Double,false,None), Field(y,Double,false,None), Field(z,Double,false,None)))
```

Once the schema has been mapped to the `CaseClass` datatype, the `showCaseClass` method can be used to output the case class scala code -
```
scala> caseClass.showCaseClass
res0: String =
"case class Export(
  _c0: Option[Int],
  carat: Option[Double],
  cut: Option[String],
  color: Option[String],
  clarity: Option[String],
  depth: Option[Double],
  table: Option[Double],
  price: Option[Int],
  x: Option[Double],
  y: Option[Double],
  z: Option[Double]
)
"
```

If desired, this code can be simply cut and pasted into the shell and the `DataFrame` can be converted to a `Dataset` -
```

scala> case class Export(
     |   _c0: Option[Int],
     |   carat: Option[Double],
     |   cut: Option[String],
     |   color: Option[String],
     |   clarity: Option[String],
     |   depth: Option[Double],
     |   table: Option[Double],
     |   price: Option[Int],
     |   x: Option[Double],
     |   y: Option[Double],
     |   z: Option[Double]
     | )
defined class Export

scala> val exportDS = exportDF.as[Export]
exportDS: org.apache.spark.sql.Dataset[Export] = [_c0: int, carat: double ... 9 more fields]
```

### Working with YAML

`spark class` can also work with YAML files (an example file using the classic diamonds dataset is provided in the root directory). After reading YAML files, the content can be parsed and converted to the `CaseClass` datatype with the following code -

`diamondsYaml.parseYaml.convertTo[CaseClass]`

(required imports: `import net.jcazevedo.moultingyaml._` and  `import sparkclass.CaseClass.YamlProtocol._`).

If table and data descriptions are provided in the YAML files, it is possible to output the case class definition with doc comments (using the `showCaseClassAll` method on `CaseClass`).

```scala
/**
 * Data model for Export: classic diamonds dataset
 *
 *  @param _c0
 *  @param carat weight of the diamond
 *  @param cut quality of the cut
 *  @param color diamond colour
 *  @param clarity diamond clarity
 *  @param depth total depth percentage
 *  @param table width of top of diamond relative to widest point
 *  @param price price in US dollars
 *  @param x length in mm
 *  @param y width in mm
 *  @param z depth in mm
 */
case class Export(
  _c0: Option[Int],
  carat: Option[Double],
  cut: Option[String],
  color: Option[String],
  clarity: Option[String],
  depth: Option[Double],
  table: Option[Double],
  price: Option[Int],
  x: Option[Double],
  y: Option[Double],
  z: Option[Double]
)
```