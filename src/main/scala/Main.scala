

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import java.io._




object Main{

	val writer = new PrintWriter(new File("/home/admin/data/result.txt" ))

	def split (line : String) : Array[String] = {
		return line.split(",")
	}

	def concatene (line : Array[String]) : Array[String] = {

		val result : Array[String]= new Array[String](line.length - 1)

		if (line(0) == "PATENT") {
			result(0) = "PATENT"
			result(1) = "NAME"
			for (i <- 2 to (line.length - 2)) {
				result(i) = line(i+1)
			}
			return result
		}

		else {
			result(0) = line(0).replace("\"" , "")
			result(1) = line(1).replace("\"" , "") + " " + line(2).replace("\"" , "")
			for (i <- 2 to (line.length - 2)) {

				result(i) = line(i+1)
			}
			return result
		}
	}

		def reformat (l : Array[String]) : String = {
			var result = ""
			for( x <- l ){
				if (x != l.last){
					result += x
					result += ","
				}
				else{
					result += x
					result += "\n"
				}
			}
			return result
		}



	def main(args : Array[String]){
		val conf = new SparkConf().setAppName("Concatene").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val dataRdd = sc.textFile("/home/admin/data/ainventor-short.csv")
		dataRdd.map (l => split(l))
		.map(l => concatene(l))
			  .map (l => reformat (l))
					.foreach(l => writer.write(l))
	}

}
