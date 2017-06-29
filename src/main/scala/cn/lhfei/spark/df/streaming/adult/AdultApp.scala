package cn.lhfei.spark.df.streaming.adult

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

/**
 * @author Hefei Li
 * 
 * useage: ./bin/spark-submit --class cn.lhfei.spark.df.streaming.adult.AdultApp --master local[1] /home/lhfei/spark_jobs/spark2-tutorial-1.0.0-SNAPSHOT.jar
 *
 */
object AdultApp {
  
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("Adult Application")
      .master("local")
      .getOrCreate();
    
    val adultDf = spark.sql("select * from adult")
    
    adultDf.show();
    
    val highDf = adultDf.select("age", "education", "marital_status", "occupation", "sex", "income")
      .filter(adultDf("income").===(">50K"))
      .groupBy("age", "education", "marital_status", "occupation", "sex", "income")
      .count();
    
    highDf.show()
    
    
    val records: RDD[Adult] = spark.read.textFile("/spark-data/adult.data").rdd
      .map[Array[String]] { line => line.split(", ") }
      .map[Adult] { records => convert2Vo(records)}
      .filter { adult => adult != null }

    highIncome(spark, records);
    
  }
  
  def convert2Vo(record: Array[String]): Adult = {
    var adult:Adult = null
    
    if(record.length != 15){
      return null;
		}
    
    var age:Integer = int2Integer(-1);
		var fnlwgt:Integer = int2Integer(-1);
		var education_num:Integer = int2Integer(-1);
		var capital_gain:Integer = int2Integer(-1);
		var capital_loss:Integer = int2Integer(-1);
		var hours_per_week:Integer = int2Integer(-1);				
    
		try {
		  age = int2Integer(Integer.parseInt(record(0)));
		} catch {
		  case e: Exception =>  age = int2Integer(-1);	
		}
		
    try {
		  fnlwgt = int2Integer(Integer.parseInt(record(2)));
		} catch {
		  case e: Exception =>  fnlwgt = int2Integer(-1);	
		}

    try {
		  education_num = int2Integer(Integer.parseInt(record(4)));
		} catch {
		  case e: Exception =>  education_num = int2Integer(-1);	
		}		

    try {
		  capital_gain = int2Integer(Integer.parseInt(record(10)));
		} catch {
		  case e: Exception =>  capital_gain = int2Integer(-1);	
		}
		
    try {
		  capital_loss = int2Integer(Integer.parseInt(record(11)));
		} catch {
		  case e: Exception =>  capital_loss = int2Integer(-1);	
		}
		
    try {
		  hours_per_week = int2Integer(Integer.parseInt(record(12)));
		} catch {
		  case e: Exception =>  hours_per_week = int2Integer(-1);	
		}
		
		    try {
		  age = int2Integer(Integer.parseInt(record(0)));
		} catch {
		  case e: Exception =>  age = int2Integer(-1);	
		}
		
		adult = new Adult(
                  age,
                  record(1 ),
                  fnlwgt,
                  record(3 ),
                  education_num,
                  record(5 ),
                  record(6 ),
                  record(7 ),
                  record(8 ),
                  record(9 ),
                  capital_gain,
                  capital_loss,
                  hours_per_week,
                  record(13),
                  record(14));
		
    return adult;
    
  }
  
  def highIncome(spark: SparkSession, records: RDD[Adult]): Unit = {
    import spark.implicits._
    
    val df = spark.createDataFrame(records, Class.forName("cn.lhfei.spark.df.streaming.adult.Adult"));
    df.show();
    df.printSchema();
    
    val high = df.select("age", "education", "marital_status", "occupation", "sex", "income")
      .filter(df("income").===(">50K"))
      .groupBy("age", "education", "marital_status", "occupation", "sex", "income")
      .count();
    
    high.show();
  }
  
}