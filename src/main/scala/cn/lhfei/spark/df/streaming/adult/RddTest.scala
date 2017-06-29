package cn.lhfei.spark.df.streaming.adult

import org.apache.spark.sql.SparkSession
import java.util.Arrays
import org.apache.spark.sql.Row

object RddTest {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("RDD Test")
          .master("local")
          .getOrCreate();
    
    val oddR = spark.sparkContext.parallelize(Seq(1, 3, 5, 7));
    val evenR = spark.sparkContext.parallelize(Seq(2, 4, 6, 8));
      
    oddR.foreach { println };
    evenR.foreach { println };
    
    oddR.reduce(inc);
    
    
    val names = spark.sparkContext.parallelize(List("Hefei", "Li", "Hello", "World"));
    val wd = names.map { x => (x, 1) }.reduceByKey(_ + _)
    
    //wd.foreach(msg);
    
    wd.map(f => ().##()).foreach { println}
    
    
  }
  
 
  
  def inc(item:Int, idx: Int): Int = {
    println(item +"<<>>"+ idx);
    return item + 1;
  }
  
  
  def concat(a:String, b: String): String = {
    println(a +"<<>>"+ b);
    return a +"_"+ b;
  }
}