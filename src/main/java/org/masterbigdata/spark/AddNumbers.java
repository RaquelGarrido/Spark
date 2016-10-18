package org.masterbigdata.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark program that sum numbers stored in an array. 
 * 
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */

public class AddNumbers {
	public static void main(String[] args) {
		// STEP 1. Create an SparkConf object
		SparkConf sparkConfigurator = new SparkConf();

		// STEP 2. Create an JavaSparkContext object
		JavaSparkContext context = new JavaSparkContext(sparkConfigurator) ;

		// STEP 3. Create an array of numbers
		Integer[] numbers = new Integer[] {3,56,364,32,236524757,775};

		// STEP 4. Convert the array to a list
		List<Integer> listOfNumbers = Arrays.asList(numbers) ;

		// STEP 5. Create a JavaRDD from the list of numbers
		JavaRDD<Integer> rddOfNumbers = context.parallelize(listOfNumbers) ;

		// STEP 6. Execute a reduce transformation
		int sum = rddOfNumbers.reduce((integer1, integer2) -> (integer1+integer2)) ;

		// STEP 7. Print the results
		System.out.println("The sum is: " + sum);

		// STEP 8. Stop the connection to Spark
		context.stop();
	}
}
