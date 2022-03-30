import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.joda.time._

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object Project1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")
    /*
    spark.sql("DROP table IF EXISTS Users")
    spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
    spark.sql("""INSERT INTO Users(Username, Password, Access) VALUES ("Admin", "1234", 1),("Evan", "4321", 0);""")
     */
    senateBuilder(spark)
    val user = logIn(spark)
    userAccess(spark, user)
    var analysing = true
    var response = ""
    while (analysing) {
      analysing = false
      println("What would you like to query? Pick a number. Enter something else to quit. ")
      println("1. How many trades did each politician make? ")
      println("2. Who made the most trades?")
      println("3. What is the average time between transaction and filing dates, by individuals and on a whole?")
      println("4. Who had the longest gap between transaction and filing date?")
      println("5. What stocks were being traded?")
      println("6. What was the most popular stock traded by politicians?")
      response = StdIn.readLine()
      if(response.charAt(0)=='1'){
        analysing = true
        var numTrades = scala.collection.mutable.Map[String, Int]()
        val names = spark.sql("SELECT First_Name, Last_Name FROM Senate;").collect().toList
        names.foreach{x => if(numTrades.contains(x(0)+" "+x(1))){numTrades(x(0)+" "+x(1))=numTrades(x(0)+" "+x(1))+1}else{numTrades += (x(0)+" "+x(1) -> 1)}}
        numTrades.foreach{x => println(x._1 +" made "+ x._2 + " trades.")}
      }
      if(response.charAt(0)=='2'){
        analysing = true
        var numTrades = scala.collection.mutable.Map[String, Int]()
        val names = spark.sql("SELECT First_Name, Last_Name FROM Senate;").collect().toList
        names.foreach{x => if(numTrades.contains(x(0)+" "+x(1))){numTrades(x(0)+" "+x(1))=numTrades(x(0)+" "+x(1))+1}else{numTrades += (x(0)+" "+x(1) -> 1)}}
        println(numTrades.maxBy(_._2)._1+ " made the most trades, with "+numTrades.maxBy(_._2)._2+" trades.")
      }
      if(response.charAt(0)=='3'){
        analysing = true
        val trades = spark.sql("SELECT Transaction_Date, Filed_Date, First_Name, Last_Name FROM Senate;").collect()
        val  tradeDates= scala.collection.mutable.Map[String, ArrayBuffer[Int]]()
        trades.foreach{x =>
          if(tradeDates.contains(x(2)+" "+x(3)))
          {tradeDates(x(2).toString+" "+x(3).toString).append(Days.daysBetween(LocalDate.parse(x(0).toString),LocalDate.parse(x(1).toString)).getDays())}
          else{tradeDates += (x(2).toString+" "+x(3).toString -> ArrayBuffer(Days.daysBetween(LocalDate.parse(x(0).toString),LocalDate.parse(x(1).toString)).getDays()))}}
        tradeDates.foreach{x => println(x._1 + " averaged " +x._2.sum/x._2.size + " days before filing. The longest they went without filing was " + x._2.max + " days.")}
      }
      if(response.charAt(0)=='4'){
        analysing = true
        val trades = spark.sql("SELECT Transaction_Date, Filed_Date, First_Name, Last_Name FROM Senate;").collect()
        val tradeDates = scala.collection.mutable.Map[String, Int]()
        trades.foreach{x => tradeDates += (x(2).toString+" "+x(3).toString -> Days.daysBetween(LocalDate.parse(x(0).toString),LocalDate.parse(x(1).toString)).getDays())}
        println("The Senator that went the longest without filing a trade is "+tradeDates.maxBy(_._2)._1+", who didn't file for "+ tradeDates.maxBy(_._2)._2 + " days.")
      }
      if(response.charAt(0)=='5'){
        analysing = true
        var numTrades = scala.collection.mutable.Map[String, Int]()
        val symbols = spark.sql("SELECT Symbol FROM Senate ;").collect().toList
        symbols.foreach{x => if(numTrades.contains(x(0).toString)){numTrades(x(0).toString)=numTrades(x(0).toString)+1}else{numTrades += (x(0).toString -> 1)}}
        numTrades.foreach{x => println(x._1 +" had "+ x._2+ " trades.")}
      }
      if(response.charAt(0)=='6'){
        analysing = true
        var numTrades = scala.collection.mutable.Map[String, Int]()
        val symbols = spark.sql("SELECT Symbol FROM Senate;").collect().toList
        symbols.foreach{x => if(numTrades.contains(x(0).toString)){numTrades(x(0).toString)=numTrades(x(0).toString)+1}else{numTrades += (x(0).toString -> 1)}}
        println("The most popular ticker is " + numTrades.maxBy(_._2)._1 + " with " + numTrades.maxBy(_._2)._2 + " trades.")
      }
    }
    println("Done.")
    spark.close()
  }
  def logIn(spark:SparkSession): String={
    println("Username: (Type new if you would like to create a new user.)")
    var userName = StdIn.readLine()
    if(userName == "new"){
      println("New username: ")
      userName = StdIn.readLine()
      println("Password: ")
      val password = StdIn.readLine()
      spark.sql("INSERT INTO Users(Username, Password, Access) VALUES (\""+userName+"\",\""+password+"\", 0);")
      println("Now login: ")
      logIn(spark)
     }else{
      println("Password: ")
      val password = StdIn.readLine()
      val users = spark.sql("SELECT Username, Password FROM Users;").collect().toList
      users.foreach{x => if( x(0)==userName && x(1)==password) return x(0).toString}
      println("Try again.")
      logIn(spark)}
  }
  def senateBuilder(spark:SparkSession): Unit={
    spark.sql("DROP table IF EXISTS Senate")
    val SenateData = spark.read.format("csv").options(Map("header"->"true", "delimiter" -> ",")).load("C:\\Users\\15084\\Desktop\\Revature\\Project1\\senate_formatted.txt")
    SenateData.write.saveAsTable("Senate")
    //spark.sql("SELECT * FROM Senate").show()
  }
  def userAccess(spark:SparkSession, user:String): Unit= {
    val access = spark.sql("SELECT Access FROM Users WHERE Users.Username =\"" + user + "\";").collect().toList
    if (access(0).toString()(1) == '1') {
      adminPrivileges(spark)
    }
    basicPrivileges(spark, user)
  }
  def basicPrivileges(spark: SparkSession, user:String): Unit = {
    var modifying = true
    while (modifying) {
      modifying = false
      println("Would you like to change your username or password? y/n")
      var response = StdIn.readLine()
      if (response.charAt(0) == 'y') {
        println("Press 1 to change your username. Press 2 to change your password. Anything else to move on.")
        response = StdIn.readLine()
        if (response.charAt(0) == '1') {
          modifying = true
          println("Enter new username: ")
          response = StdIn.readLine()
          val userTable = spark.sql("SELECT *  FROM Users;").collect().toList
          spark.sql("DROP TABLE Users;")
          spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
          var sqlStr = "INSERT INTO Users(Username, Password, Access) VALUES "
          userTable.foreach { x => if (x(0) == user) {
            sqlStr = sqlStr.concat("(\"" + response + "\",\"" + x(1) + "\"," + x(2) + "),")
          } else {
            sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\"," + x(2) + "),")
          }
          }
          sqlStr = sqlStr.patch(sqlStr.lastIndexOf(','), ";", 1)
          spark.sql(sqlStr)
          spark.sql("SELECT * FROM Users WHERE Users.Username = \"" + response + "\";").show()
        }
        if (response.charAt(0) == '2') {
          modifying = true
          println("Enter new password: ")
          response = StdIn.readLine()
          val userTable = spark.sql("SELECT *  FROM Users;").collect().toList
          spark.sql("DROP TABLE Users;")
          spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
          var sqlStr = "INSERT INTO Users(Username, Password, Access) VALUES "
          userTable.foreach { x => if (x(0) == user) {
            sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + response + "\"," + x(2) + "),")
          } else {
            sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\"," + x(2) + "),")
          }
          }
          sqlStr = sqlStr.patch(sqlStr.lastIndexOf(','), ";", 1)
          spark.sql(sqlStr)
          spark.sql("SELECT * FROM Users WHERE Users.Username = \"" + user + "\";").show()
        }
      }
    }
  }
  def adminPrivileges(spark:SparkSession): Unit ={
    println("Would you like to modify users? y/n")
    var response = StdIn.readLine()
    if (response.charAt(0) == 'y') {
      var modifying = true
      while(modifying) {
        modifying = false
        spark.sql("SELECT Username, Access FROM Users;").show()
        println("Press 1 to promote a user's access level, press 2 to demote, press 3 to delete a user. Anything else to finish modifying users.")
        response = StdIn.readLine()
        if (response.charAt(0) == '1') {
          promotion(spark)
          modifying = true
        }
        if (response.charAt(0) == '2') {
          demotion(spark)
          modifying = true
        }
        if (response.charAt(0) == '3') {
          deletion(spark)
          modifying = true
        }
      }
    }
  }
  def promotion(spark: SparkSession): Unit={
    println("Type the user you would like to promote: ")
    val response = StdIn.readLine()
    val users = spark.sql("SELECT Username FROM Users;").collect().toList
    var realName = false
    users.foreach { x => if (x(0) == response) {realName = true}}
    if (realName) {
      val userTable = spark.sql("SELECT *  FROM Users;").collect().toList
      spark.sql("DROP TABLE Users;")
      spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
      var sqlStr = "INSERT INTO Users(Username, Password, Access) VALUES "
      userTable.foreach { x => if (x(0) == response) {sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\",1),")} else {sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\"," + x(2) + "),")}}
      sqlStr = sqlStr.patch(sqlStr.lastIndexOf(','), ";", 1)
      spark.sql(sqlStr)
      println("Promoted "+ response)
    }
    else {println("Not a user.")}
  }
  def demotion(spark: SparkSession): Unit ={
    println("Type the user you would like to demote: ")
    val response = StdIn.readLine()
    val users = spark.sql("SELECT Username FROM Users;").collect().toList
    var realName = false
    users.foreach { x => if (x(0) == response) {realName = true}}
    if (realName) {
      val userTable = spark.sql("SELECT *  FROM Users;").collect().toList
      spark.sql("DROP TABLE Users;")
      spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
      var sqlStr = "INSERT INTO Users(Username, Password, Access) VALUES "
      userTable.foreach { x => if (x(0) == response) {sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\",0),")} else {sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\"," + x(2) + "),")}}
      sqlStr = sqlStr.patch(sqlStr.lastIndexOf(','), ";", 1)
      spark.sql(sqlStr)
      println("Demoted "+ response)
    }
    else {println("Not a user.")}
  }
  def deletion(spark: SparkSession): Unit ={
    println("Type the user you would like to delete: ")
    val response = StdIn.readLine()
    val users = spark.sql("SELECT Username FROM Users;").collect().toList
    var realName = false
    users.foreach { x => if (x(0) == response) {realName = true}}
    if (realName) {
      val userTable = spark.sql("SELECT *  FROM Users;").collect().toList
      spark.sql("DROP TABLE Users;")
      spark.sql("Create table IF NOT EXISTS Users(Username String, Password String, Access Int);")
      var sqlStr = "INSERT INTO Users(Username, Password, Access) VALUES "
      userTable.foreach { x => if (x(0) == response){println("Deleted "+ response)} else {sqlStr = sqlStr.concat("(\"" + x(0) + "\",\"" + x(1) + "\"," + x(2) + "),")}}
      sqlStr = sqlStr.patch(sqlStr.lastIndexOf(','), ";", 1)
      spark.sql(sqlStr)
    }
    else{println("Not a user.")}
  }
}
