

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Main{

	val writer = new PrintWriter(new File("/home/admin/data/result.txt" ))

	def concatene (line : Array[String]) : Array[String] = {

		val result = new Array[String](line.length - 1)

		if (line(0) == "PATENT") {
			result(0) = "PATENT"
			result(1) = "NAME"
			for (i <- 2 to (line.length - 2)) {
				result(i) = line(i)
			}
			return result
		}

		else {
			result(0) = line(0)
			result(1) = line(1) + " " + line(2)
			for (i <- 3 to (line.length - 2)) {

				result(i) = line(i)
			}
			return result
		}


	}

	def main(args : Array[String]){
		val conf = new SparkConf().setAppName(appName).setMaster(master)
		val sc = new SparkContext(conf)
		val dataRdd = sc.textFile("/home/admin/data/ainventor-short.csv")
		dataRdd.map(l => concatene(l.split(",")))
		dataRdd.foreach( l => writer.write(l))


	}


}
