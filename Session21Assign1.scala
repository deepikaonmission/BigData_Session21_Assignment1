//<<<<<<<<<---------- TASK 1 --------->>>>>>>>>
//Counting blank lines in a txt file using accumulator

import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}  //Explanation is already given in Assignment18.1

object Session21Assign1 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session21Assign1")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 21/Assignments/Assignment1")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  //setting path of winutils.exe
  System.setProperty("hadoop.home.dir", "F:/Softwares/winutils")
  //winutils.exe needs to be present inside HADOOP_HOME directory, else below error is returned:
  //error: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

  //reading Sample.txt file and storing in rdd1
  val rdd1 = spark.sparkContext.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 21/Assignments/Assignment1/Sample.txt")
  //rdd -->> RDD[String]
  rdd1.foreach(x => println(x))           //printing each line of rdd1
  //**********REFER Screenshot 1 for OUTPUT***********

  //First method of using accumulator ----------------->>>>>>>>>>>>>>>
  //creating accumulator of type Int
  var accum = spark.sparkContext.accumulator(0,"CountBlankLinesAccum")    //accum -->> Accumulator[Int]
  //counting blank lines
  rdd1.foreach{x => if(x.length == 0) accum+=1}
  //printing value of accumulator using value method
  println(s"Number of blank lines are ${accum.value}")
  //***********REFER Screenshot 2 for OUTPUT***********

  //Second method of using accumulator ----------------->>>>>>>>>>>>>>>
  //creating accumulator of type LongAccumulator
  var accum1 = spark.sparkContext.longAccumulator("CountBlankLinesAccum1")  //accum1 -->> LongAccumulator
  //counting blank lines
  rdd1.foreach{x => if(x.length == 0) accum1.add(1)}
  //printing value of accumulator using value method
  println(s"Number of blank lines are ${accum1.value}")
  //***********REFER Screenshot 3 for OUTPUT***********

}
