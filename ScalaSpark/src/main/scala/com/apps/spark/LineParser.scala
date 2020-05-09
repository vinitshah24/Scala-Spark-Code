package com.apps.spark

import org.apache.spark.sql.SparkSession

object LineParser {

  def parse(line: String) = {
    val splitLine = line.split(',')
    val name = splitLine(0).toString
    val id = splitLine(1).toInt
    val score = splitLine.slice(2, 5).map(x => x.toDouble)
    val enrolled = splitLine(6).toBoolean
    (name, id, score, enrolled)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Line Parser")
      .master("local")
      .enableHiveSupport().getOrCreate()
    val data = "Vinit, 74835930, 80, 90, 99, 95, 85, true"
    val outputTuple = parse(data)
    val studentName = outputTuple._1.toUpperCase
    val studentID = outputTuple._2
    val studentScores = outputTuple._3.toArray //Also can transform .toSet
    val isEnrolled = outputTuple._4
  }
}
