package com.apps.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{explode, explode_outer, posexplode, posexplode_outer}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

object Explode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkApplication")
      .master("local")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val arrayData = Seq(
      Row("James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
      Row("Michael", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null)),
      Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
      Row("Washington", null, null),
      Row("Jefferson", List(), Map())
    )
    val arraySchema = new StructType().
      add("name", StringType).
      add("knownLanguages", ArrayType(StringType)).
      add("properties", MapType(StringType, StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
    df.printSchema()
    df.show(false)
    /*
    +----------+--------------+-----------------------------+
    |name      |knownLanguages|properties                   |
    +----------+--------------+-----------------------------+
    |James     |[Java, Scala] |[hair -> black, eye -> brown]|
    |Michael   |[Spark, Java,]|[hair -> brown, eye ->]      |
    |Robert    |[CSharp, ]    |[hair -> red, eye -> ]       |
    |Washington|null          |null                         |
    |Jefferson |[]            |[]                           |
    +----------+--------------+-----------------------------+
     */

    // explode -> Array
    df.select($"name", explode($"knownLanguages")).show(false)
    /*
    +-------+------+
    |name   |col   |
    +-------+------+
    |James  |Java  |
    |James  |Scala |
    |Michael|Spark |
    |Michael|Java  |
    |Michael|null  |
    |Robert |CSharp|
    |Robert |      |
    +-------+------+
     */

    //
    df.select($"name", explode_outer($"knownLanguages")).show(false)
    /*
    +----------+------+
    |name      |col   |
    +----------+------+
    |James     |Java  |
    |James     |Scala |
    |Michael   |Spark |
    |Michael   |Java  |
    |Michael   |null  |
    |Robert    |CSharp|
    |Robert    |      |
    |Washington|null  |
    |Jefferson |null  |
    +----------+------+
     */

    // posexplode -> Array
    df.select($"name", posexplode($"knownLanguages")).show(false)
    /*
    +-------+---+------+
    |name   |pos|col   |
    +-------+---+------+
    |James  |0  |Java  |
    |James  |1  |Scala |
    |Michael|0  |Spark |
    |Michael|1  |Java  |
    |Michael|2  |null  |
    |Robert |0  |CSharp|
    |Robert |1  |      |
    +-------+---+------+
     */

    // posexplode –> Map
    df.select($"name", posexplode($"properties")).show(false)
    /*
    +-------+---+----+-----+
    |name   |pos|key |value|
    +-------+---+----+-----+
    |James  |0  |hair|black|
    |James  |1  |eye |brown|
    |Michael|0  |hair|brown|
    |Michael|1  |eye |null |
    |Robert |0  |hair|red  |
    |Robert |1  |eye |     |
    +-------+---+----+-----+
     */

    // posexplode_outer –> map
    df.select($"name", posexplode_outer($"properties")).show(false)
    /*
    +----------+----+----+-----+
    |name      |pos |key |value|
    +----------+----+----+-----+
    |James     |0   |hair|black|
    |James     |1   |eye |brown|
    |Michael   |0   |hair|brown|
    |Michael   |1   |eye |null |
    |Robert    |0   |hair|red  |
    |Robert    |1   |eye |     |
    |Washington|null|null|null |
    |Jefferson |null|null|null |
    +----------+----+----+-----+
     */

  }
}
