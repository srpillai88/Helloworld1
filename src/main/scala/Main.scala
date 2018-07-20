import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object Run {
  def main(args: Array[String]){
    println("Hello World")

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile: RDD[String] = sc.parallelize(Seq("Hello Hello world eg txt"))

    //word count
    val counts: RDD[(String, Int)] = textFile.flatMap(line => line.split(" "))
      .map(word => (word,1))
      .reduceByKey((x,y)=> x+y)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    counts.saveAsTextFile("/tmp/shakespeareWordCount");
  }

}
