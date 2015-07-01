package org.cg.spark_lab


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestSpark {

	def main(args: Array[String]) {

		val conf = new SparkConf().setAppName("scala_spark_test").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
		distData.collect().foreach(println)

	}


}