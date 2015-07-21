package spark_lab_java8;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestSpark {
	
	public static void main(String args[]){
		
		SparkConf conf = new SparkConf().setAppName("sparkTest").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("data/data.txt");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		System.out.println(totalLength);
		
	}

}
