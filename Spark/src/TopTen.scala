import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.LocalDate;

object TopTen {
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("TopTen").setMaster("local")
    val sc = new SparkContext(config);

    val journal = sc.textFile("file:///home/arthur/workspace/scala/HW2/soc-LiveJournal1Adj.txt")
    val topTenRes = journal
    .flatMap(formatJournal)
    .reduceByKey((x, y) => x intersect(y))
    .map(x => (x._2.size, x._1))
    .sortByKey(false)
    .map(x => (x._2, x._1))
    .zipWithIndex().filter(_._2 < 10)
    .map(x => x._1)
        
    val userdata = sc.textFile("file:///home/arthur/workspace/scala/HW2/userdata.txt")
    val userInfo = userdata.flatMap(formatUserData)
    
    val topTenFormat = topTenRes.flatMap(formatTopTen)
    
    val joinRes = topTenFormat.join(userInfo)
        
    val reduceRes = joinRes.flatMap(formatJoinRes)
    // ("user1,user2:count", (userId_info)) => ("count user1_info user2_info")
    val result = reduceRes.reduceByKey(_ + "\t" + _).map(x => x._1.split(":")(1) + "\t" + x._2)
    result.saveAsTextFile("file:///home/arthur/workspace/scala/HW2/output")
  }
  
  // ("userId", ("user1,user2:count", (userId_Info))) => ("user1,user2:count", (userId_info))
  def formatJoinRes(line : (String, (String, List[String]))) : List[(String, String)] = {
    val user = line._1
    val key = line._2._1
    val value = line._2._2(0) + "\t" + line._2._2(1) + "\t" + line._2._2(2)
    List((key, value))
  }
  
  // (user1, ("user1,user2:count")), (user2, ("user1,user2:count")) 
  def formatTopTen(line : (String, Int)) : List[(String, String)] = {
    val keys = line._1.split(",")
    List((keys(0), line._1 + ":" + line._2), ((keys(1), line._1 + ":" + line._2)))
  }
  
  def formatJournal(line : String) : List[(String,List[Int])] = {
    val userToFriendsPairs = line.split("\\t")
    if (userToFriendsPairs.size == 2) {
      val user = userToFriendsPairs(0).toInt
      val friends = userToFriendsPairs(1).split(",").map(_.toInt).toList
      val pair = friends.map(friend => {
        if (user < friend) user + "," + friend else friend + "," + user
      })
      pair.map(key => (key, friends))
    } else List()
  }
  
  def formatUserData(line : String) : List[(String, List[String])] = {
    val userInfo = line.split(",")
    List((userInfo(0), List(userInfo(1), userInfo(4), getAge(userInfo(9)))))
  }
  
  def getAge(dob : String) : String = {
      val info = dob.split("/")
			val month = info(0).toInt
			val day = info(1).toInt
			val year = info(2).toInt
			var age = LocalDate.now().getYear() - year
			if (month > LocalDate.now().getMonthValue()) {
				age-= 1
			} else if (month == LocalDate.now().getMonthValue()) {
				if (day > LocalDate.now().getDayOfMonth()) age-= 1;
			}
			age.toString()
  }
}