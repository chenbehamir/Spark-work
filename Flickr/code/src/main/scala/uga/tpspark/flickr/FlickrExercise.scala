package uga.tpspark.flickr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object FlickrExercise {
  def main(args: Array[String]): Unit = {
    
    // executing Spark on your machine, using 6 threads
    val conf = new SparkConf().setMaster("local[6]").setAppName("Flickr batch processing")
    val sc = new SparkContext(conf)

    // Control our logLevel. we can pass the desired log level as a string.
    // Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")
    
    try {
     
     
      val originalFlickrMetaData: RDD[String] = sc.textFile("flickrSample.txt")
    
      ///--------------------------------  QUESTION 1 ------------------------
      println("\n--------------------------------  QUESTION 1 ------------------------\n")
      
      println("First 5 lines in the RDD: \n")
      
      originalFlickrMetaData.take(5).foreach(println)  //dispaly the 5 lines of the RDD
      
      println("\n Number of elements= "+originalFlickrMetaData.count())          //display the number of elements in the RDD
      
      
      println("---------------------------------------------------------------------")
     
      ///--------------------------------  QUESTION 2  ---------------------      
    
      println("\n--------------------------------  QUESTION 2 ------------------------\n")
    
      def stringToPicture (s: String) :Picture =                   //convert a string element to a picture
     {
        val array : Array[String] = s.split("\t")
        val pic = new Picture(array)
        return pic
     }
   
     val pictureRDD : RDD[Picture] = (originalFlickrMetaData.map(f=>stringToPicture(f))).filter(pic => (pic.hasValidCountry && pic.hasTags))
     
     println("\nSome lines in the RDD: \n")
     pictureRDD.take(5).foreach(println)
     
     println("---------------------------------------------------------------------")
     
     //-------------------------------------- QUESTION 3 ------------------------
     
     println("\n--------------------------------  QUESTION 3 ------------------------\n")
      
     val t0 = System.nanoTime()   // Timer to compute the execution time
     
     val picCountryRDD = pictureRDD.groupBy(pic=>pic.c)
    
     println("\nFirst line in the RDD: \n")
     picCountryRDD.take(1).foreach(println)
    
     
     println("\n The type of this RDD is:  RDD[(Country, Iterable[Picture])]")
     
     println("---------------------------------------------------------------------")
    
     //-------------------------------------- QUESTION 4 -------------------------
     
     println("\n--------------------------------  QUESTION 4 ------------------------\n")
     
     val countryTagsRDD = (pictureRDD.map(pic => (pic.c, pic.userTags.toList))).combineByKey(x => x , 
           (s: List[String], x: List[String]) => s ++: x,
           (s: List[String], x: List[String]) => s ++: x)
     
           println("The lines in the RDD: \n")
           countryTagsRDD.take(4).foreach(println)
     
     println("---------------------------------------------------------------------")      
     
     //-------------------------------------- QUESTION 5 ----------------------
    
     println("\n--------------------------------  QUESTION 5 ------------------------\n")
     
     def listToMapCount (list: List[String]) = // get a (tag, frequency) for each tag
     {
      list.groupBy(l=>l).map(t=> (t._1, t._2.length))
     }
           
    val noRepTagsRDD = countryTagsRDD.map( x => (x._1, listToMapCount(x._2)))  
   
    println("The lines in the RDD: \n")
    noRepTagsRDD.take(4).foreach(println)
    
    val t1 = System.nanoTime()   // end of the question 5
    println("\nElapsed time for this method: " + (t1 - t0) + "ns")
    
    println("---------------------------------------------------------------------")
    //-------------------------------------- QUESTION 6 ------------------------
    println("\n--------------------------------  QUESTION 6 ------------------------\n")
    
    val t2 = System.nanoTime()  // start time for question 6
    
    //function that take two list as input and return one 
    //list of pairs(val,number of frequency of val)
    def reduceConcatList(list1: List[String]) :List[(String,Int)]=
    {
    var list:List[(String,Int)]=null;
       list= list1.map(elm=>(elm,list1.count(_== elm)))
     return list;
    }

    
    val newRDD= pictureRDD.map(x=>(x.c,x.userTags.toList)).reduceByKey(_:::_).map(x=>(x._1,reduceConcatList(x._2).toSet.toMap))
   
    println("\nThe lines in the RDD: \n")
    newRDD.take(4).foreach(println)
  

     val t3 = System.nanoTime()   // end time for question6
     println("\nElapsed time for question 6: " + (t3 - t2) + "ns")
     
     println("---------------------------------------------------------------------")
    //----------------------------------------------------
    } catch {
      case e: Exception => throw e
    } finally {
      sc.stop()
    }
    println("done")
  }
}