package org.cg.spark_lab

import scala.io.Source
import java.io._
/**
 * @author ${user.name}
 */
object App {

  var count = 0
  def currentCount(): Long = {
    count += 1
    count
  }

  def main(args: Array[String]) {

    val filename = "data/kddcup.data_10_percent"
    val pw = new PrintWriter(new File("data/kddcup.data_0.1_percent"))

    for (line <- Source.fromFile(filename).getLines()) {
      currentCount()
      if (count % 100 == 0)
        pw.write(line + "\n")
    }
    pw.close()
    println(count)
  }

}
