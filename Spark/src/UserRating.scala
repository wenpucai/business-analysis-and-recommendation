import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object UserRating {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("UserDating").setMaster("local")
    val sc = new SparkContext(config);

    val business = sc.textFile("file:///home/arthur/workspace/scala/HW2/business.csv")
    val review = sc.textFile("file:///home/arthur/workspace/scala/HW2/review.csv")
    val user = sc.textFile("file:///home/arthur/workspace/scala/HW2/user.csv")
    val businessData = business
    .map(line => line.split("::"))
    .map(cols => (cols(0), cols(2)))
    
    val reviewData = review
    .map(line => line.split("::"))
    .map(cols => (cols(2), (cols(1), cols(3))))
    
    val userData = user
    .map(line => line.split("::"))
    .map(cols => (cols(0), cols(1)))
    
    val join = businessData.join(reviewData)
    .filter(_._2._1.contains("Colleges & Universities"))
    .map(x => (x._2._2._1, x._2._2._2))
    .join(userData)
    .map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1)
    
    join.saveAsTextFile("file:///home/arthur/workspace/scala/HW2/output")
  }
}