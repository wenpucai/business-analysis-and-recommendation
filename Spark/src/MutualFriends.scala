import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MutualFriends {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("MutuaclFriends").setMaster("local")
    val sc = new SparkContext(config);

    val rdd = sc.textFile("file:///home/arthur/workspace/scala/HW2/soc-LiveJournal1Adj.txt")
    val result = rdd
    .flatMap(format)
    .reduceByKey((x, y) => x intersect(y))
    .map(x => x._1 + "\t" + x._2.size)
    result.saveAsTextFile("file:///home/arthur/workspace/scala/HW2/output")
  }
  
  def format(line : String) : List[(String,List[Int])] = {
    val userToFriendsPairs = line.split("\\t");
    if (userToFriendsPairs.size == 2) {
      val user = userToFriendsPairs(0).toInt
      val friends = userToFriendsPairs(1).split(",").map(_.toInt).toList
      val pair = friends.map(friend => {
        if (user < friend) user + "," + friend else friend + "," + user
      })
      pair.map(key => (key, friends))
    } else List()
  }
}