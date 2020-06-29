package ml

import org.apache.spark._
//import org.elasticsearch.spark.rdd.EsSpark


// case class UserIndex(departure: String, arrival: String)
//
//val row = UserIndex("OTP", "SFO")
//
//val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
//EsSpark.saveToEs(rdd, "spark/docs")


object RealtimeTransform {
  def addDate (line:String):String={

    val date=java.time.LocalDate.now().toString.replace("-","/");
    return line.substring(line.indexOf(",") + 1)+","+date
  }

  def segmentAge (line:String):String={
   val age=line.split(",")(0).toInt;
    var ageGroup="";
    if(age<25){
      ageGroup="18-25"
    } else if(age<30){
      ageGroup="25-30"
    } else if(age<40){
      ageGroup="30-40"
    } else if(age<60){
      ageGroup="40-60"
    }

    return ageGroup+","+line.substring(line.indexOf(",") + 1)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input =  sc.textFile("D:\\events.txt")
    val inputuser =  sc.textFile("D:\\user.txt")
    val userMap=inputuser.map(line =>(line.split(",")(0),line.substring(line.indexOf(",") + 1)))
    val eventsMap = input.map(word => (word.split(",")(0), addDate(word)))

    val userAggerigated= userMap.map(line => line._1+","+segmentAge(line._2) )

    val joinMap =eventsMap.join(userMap)

    val keylessMap = joinMap.map(line => line._1+","+line._2._1+","+line._2._2)

    userAggerigated.foreach(println);

    keylessMap.foreach(println);

    keylessMap.saveAsTextFile("D:\\EC\\Out\\eventIndex.txt")
    userAggerigated.saveAsTextFile("D:\\EC\\Out\\userIndex.txt")
  }
}