package org.cg.spark_lab

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.collection.mutable.ArrayBuffer

object StreamingKMeans {

  def isAllDigits(x: String) = x forall Character.isDigit
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKMeans").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))
   // val hosts =List(9993,9999).map(i=>ssc.socketTextStream("localhost", i))
   // val unifiedStream = ssc.union(hosts)  
   // hosts.map(a=>a.print())
    // val trainingData = ssc.textFileStream("/Users/yliu/spark-lab/spark-lab/data_pool").map(LabeledPoint.parse)
    //val testData = ssc.textFileStream("/Users/yliu/spark-lab/spark-lab/data_pool").map(LabeledPoint.parse)
   
   
    val trainingDataStream = ssc.socketTextStream("localhost", 9993);
    val testDataStream = ssc.socketTextStream("localhost", 9999);

    val trainingData = trainingDataStream.map(_.split(",")).filter(a => a.size>1).map{a=>
       val vector = Vectors.dense(a.map(_.toDouble))
       vector
    }
    
    val testData = testDataStream.map(_.split(",")).filter(a => a.size>1).map{a=>
      val label = a(0).toDouble
      val buff = a.toBuffer
      buff.remove(0)
      val feature = buff.map(_.toDouble).toArray
      val lb = LabeledPoint(label, Vectors.dense(feature))
      lb
    }
    
    trainingData.print()
    
    testData.print()
 
   
   
    val numDimensions = 3
    val numClusters = 2
    val model = new StreamingKMeans()
    .setK(numClusters)
    .setDecayFactor(1.0)
    .setRandomCenters(numDimensions, 0.0)
    .setInitialCenters(trainingData)
    
    model.trainOn(trainingData)
    
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    
    
    ssc.start()
    ssc.awaitTermination()
  }

}