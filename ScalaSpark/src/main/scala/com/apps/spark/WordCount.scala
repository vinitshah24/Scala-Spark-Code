package com.apps.spark

import org.apache.spark.sql.{SparkSession, Row}

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Examples").enableHiveSupport().getOrCreate()
    val textFile = spark.read.text("").rdd
    val file = textFile.map { case Row(s: String) => s }
    file.collect()
    //Transforming individual String
    val split = file.flatMap(line => line.split(""))
    split.collect()
    val clean = split.map(_.replaceAll("[, .]]", ""))
    val map = clean.map(word => (word, 1))
    val reduce = map.reduceByKey(_ + _)
    val sort = reduce.sortBy(_._2)
    val removeCompactBuffer = sort.groupBy(_._2).mapValues(_.map(_._1).mkString("(", ",", ")"))
    removeCompactBuffer.collect().foreach(println)

    val sentence = Array("This is just a sample array string")
    val mapArray = sentence.map(l => l.split(" ")).map(_.distinct)
  }
}
