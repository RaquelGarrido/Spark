package org.masterbigdata.spark;

import java.awt.TextField;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Function;
import scala.Function2;
import scala.Tuple2;

import com.esotericsoftware.minlog.Log.Logger;

/**
 * Taking as a reference the program that count works in Slide 8 of Lesson 4,
 *  write a program called SparkWordCountWithAnonymousFunctions.java that perform the count of words but using anonymous functions
 *  instead of lamdba expressions.
 *  The output of the programs must be list of tuples ordered by the number of words.Spark program that sum numbers stored in files. 
 *  
 * @author Raquel Garrido Ayuso
 */
public class SparkWordCountWithAnonymousFunctions {
	
	
	public static void main( String[] args ) {
				
		// STEP 1. Create an SparkConf object
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark Word count");
		
		// STEP 2. Create an JavaSparkContext
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf) ;
		
		
		List<Tuple2<String,Integer>> output1 = (List<Tuple2<String, Integer>>) sparkContext;
		
		// STEP 3. Read the contents of the file/directory passed in the first argument (.textFile(args[0])
		
				if (args.length != 1) {
					throw new RuntimeException("Missing argument") ;
				}
			    JavaRDD<String> lines = sparkContext.textFile(args[0]) ;
		
		//STEP 4. Each input is mapped to 0 or more output items(.flatMap(s-> Arrays.asList(s.split(" ")).iterator()))
			    
		JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String,String>() {
				
		public Iterator<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" ")).iterator();
		}
		});
		
    	//STEP 5. Map Transformation to convert String to Tuple (.mapToPair(s-> new Tuple2<String,Integer>(s, 1)))
    	
    	JavaPairRDD <String,Integer> ones = word.mapToPair(new PairFunction<String, String, Integer>() {
    		
    		public Tuple2<String,Integer> call (String s) throws Exception {
    			return new Tuple2(s,1);
    		}
    	});
    	//STEP 6. Reduce action to count words (.reduceByKey((integer,integer2)-> integer+integer2))
    	
    	JavaPairRDD<String,Integer> counts = ones.reduceByKey(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1+i2;
			}
	
       	}
    	);
    
    	//STEP 7. Sort the words (.sortByKey())
    	
    	JavaPairRDD<String, Integer> sortCount= counts.sortByKey();
    	
       //STEP 8. Collect the words (.collect())
    	
    	List<Tuple2<String, Integer>> output = sortCount.collect();

        for (Tuple2<?,?> tuple : output) {

          System.out.println(tuple._1() + ": " + tuple._2());

        }
    
        // STEP 10. Stop the connection to Spark
        sparkContext.stop();
      }
		
	}

