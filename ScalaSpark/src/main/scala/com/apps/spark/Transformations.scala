package com.apps.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Transformations {

  case class ArtistsClass(id: Int, name: String, country: String)

  case class AlbumsClass(name: String, releaseYear: Int, copiesSold: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkApplication")
      .master("local")
      .enableHiveSupport().getOrCreate()

    import spark.implicits._
    val data = Seq(
      (1, "Future", "USA"),
      (2, "Drake", "Canada"),
      (3, "Dave", "UK"),
      (4, "Weeknd", "Canada"),
      (5, "JayZ", "USA")
    ).toDF("id", "name", "country")

    //RDD's
    val artists = data.rdd
    val rdd1 = artists.map(s => s(1).toString.substring(0, 2))
    val rdd2 = artists.map(s => (s(1), s(1).toString.length))
    rdd2.collect.foreach(println)

    //Converting RDD to +DataFrame [Schema Method 1]
    val rowForDF1 = artists.map(s => Row(s(0), s(1), s(2)))
    val schema1 = List(
      StructField("ID", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = true),
      StructField("Country", StringType, nullable = true)
    )
    val artistsDF1 = spark.createDataFrame(rowForDF1, StructType(schema1))
    artistsDF1.show()

    //Converting RDD to DataFrame [Schema Method 2]
    val rowForDF2 = artists.map(s => Row(s(0), s(1), s(2)))
    val schema2 = StructType(
      StructField("ID", IntegerType, nullable = false) ::
        StructField("Name", StringType, nullable = false) ::
        StructField("Country", StringType, nullable = false) :: Nil
    )
    val artistsDF2 = spark.createDataFrame(rowForDF2, schema2)
    artistsDF2.show()

    //Converting RDD to DataFrame [Schema Method 3]
    val artistsRDD = data.rdd
    val schemaString = "ID Name Country"
    val schemaFromString = StructType(
      schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
    )
    val rappersData = spark.createDataFrame(artistsRDD, schemaFromString)
    rappersData.show()

    //Converting DataFrame to DataSet
    import spark.implicits._
    val artistsDS: Dataset[ArtistsClass] = data.as[ArtistsClass]
    artistsDS.show()

    //Converting DataSet to DataFrame
    // org.apache.spark.sql.Dataset[Long] = [id: bigint]
    val createDS = spark.range(5)
    createDS.show()
    //org.apache.spark.sql.DataFrame = [id: bigint]
    val createDF = spark.range(5).toDF
    createDF.show()

    //RDD -> DataFrame -> DataSet
    val albumsSeq = Seq(
      AlbumsClass("DS2", 2002, 20000),
      AlbumsClass("Views", 2016, 40000),
      AlbumsClass("Psychodrama", 2019, 1000)
    )
    val albumRDD = spark.sparkContext.parallelize(albumsSeq, 2)
    val albumDF = albumRDD.toDF()
    val albumDS = albumDF.as[AlbumsClass]
    albumDS.show()

    //Create RDD
    val CanadianArtistsRDD = spark.sparkContext.parallelize(
      Seq(
        (2, "Drake"), (1, "Weeknd"), (3, "Nav")
      ))
    val sortedArtistsRdd = CanadianArtistsRDD.sortByKey()

    //RDD to DF
    val artistsDF = sortedArtistsRdd.toDF("ID", "Name")
    println(artistsDF.columns.array.length)
    artistsDF.dropDuplicates().show()
  }
}
