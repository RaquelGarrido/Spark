package org.masterbigdata.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Spark program that sum numbers stored in files. 
 * The program has an argument that must fit into a file or a directory containing files. Each file 
 * must contain a number per line.
 * Version with private classes
 * 
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */
public class AddNumbersFromFilesWithAnonymousFunctions {
	
	public static void main( String[] args ) {
		// STEP 1. Create an SparkConf object
		SparkConf sparkConfigurator = new SparkConf();
		
		// STEP 2. Create an JavaSparkContext object
		JavaSparkContext context = new JavaSparkContext(sparkConfigurator) ;
		
		// STEP 3. Read the contents of the file/directory passed in the first argument
		if (args.length != 1) {
			throw new RuntimeException("Missing argument") ;
		}
	    JavaRDD<String> lines = context.textFile(args[0]) ;
		
	    // STEP 4. Map transformation to convert the lines to numbers
		JavaRDD<Integer> rddOfIntegers = lines.map(new Function<String, Integer>() {
			//@Override
			public Integer call(String s) throws Exception {
				return Integer.valueOf(s);
			}
		}) ; 		
        
		// STEP 5. Reduce action to sum the numbers 
		int sum = rddOfIntegers.reduce(new Function2<Integer, Integer, Integer>() {			
			//@Override
			public Integer call(Integer integer1, Integer integer2) throws Exception {
				return integer1 + integer2 ;
			}
		}) ;

		// STEP 6. Print the results
        System.out.println("The sum is: " + sum);
        
        // STEP 7. Stop the connection to Spark
        context.stop();
      }
}
