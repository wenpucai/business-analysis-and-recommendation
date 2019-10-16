import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TailTwenty {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("TailTwenty").setMaster("local")
    val sc = new SparkContext(config);

    val business = sc.textFile("file:///home/arthur/workspace/scala/HW2/business.csv")
    val review = sc.textFile("file:///home/arthur/workspace/scala/HW2/review.csv")
    val user = sc.textFile("file:///home/arthur/workspace/scala/HW2/user.csv")
    val businessData = business
    .map(line => line.split("::"))
    .map(cols => (cols(0), (cols(1), cols(2))))
    
    val reviewData = review
    .map(line => line.split("::"))
    .map(cols => (cols(2), cols(3)))
    
    val avgRatings = reviewData
    .map(x => (x._1, (x._2.toFloat, 1)))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .map(x => (x._1, x._2._1 / x._2._2))
    
    val join = businessData.join(avgRatings)
    .distinct()
    .filter(x => x._2._1._1.contains("NY"))
    .sortBy(x => x._2._2)
    .zipWithIndex().filter(_._2 < 20)
    .map(x => x._1)
    .map(x => x._1 + "\t" + x._2._1._1 + "\t" + x._2._1._2 + "\t" + x._2._2)
    
    join.saveAsTextFile("file:///home/arthur/workspace/scala/HW2/output")
  }
}