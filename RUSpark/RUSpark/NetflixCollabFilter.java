package com.RUSpark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NetflixCollabFilter {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixCollabFilter <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
    SparkSession spark = SparkSession.builder().appName("NetflixCollabFilter").getOrCreate();
    Dataset<Row> df = spark.read().csv(InputPath);

    String[] newColumnNames = {"movie_id","customer_id","rating","date"};

    for (int i = 0; i < newColumnNames.length; i++) {
        df = df.withColumnRenamed(df.columns()[i], newColumnNames[i]);
    }
		
    Dataset<Row> meanRatings = df
    .groupBy("customer_id")
    .agg(avg("rating").alias("mean_rating"));

    Dataset<Row> joinedRating = df
    .join(meanRatings, "customer_id")
    .withColumn("mean_centered_rating", col("rating").minus(col("mean_rating")))
    .na().fill(0, new String[]{"mean_centered_rating"});

    Dataset<Row> userCombinations = df.select("customer_id")
    .distinct()
    .crossJoin(df.selectExpr("customer_id as customer_id_u").distinct())
    .filter("customer_id < customer_id_u");

    userCombinations = userCombinations
    .withColumnRenamed("customer_id", "user_u")
    .withColumnRenamed("customer_id_u", "user_v");

    // userCombinations.show();
    // joinedRating.show();

    Dataset<Row> groupedDF = joinedRating.groupBy("customer_id")
    .agg(
        concat_ws(", ", collect_list(concat(lit("{"), col("movie_id"), lit(", "), col("mean_centered_rating"), lit("}"))))
            .alias("movies_and_ratings")
    );


    // groupedDF.show();
    HashMap<Integer, String> mp = new HashMap<>(); 

    groupedDF = groupedDF.withColumn("customer_id", col("customer_id").cast("integer"));

    
    JavaRDD<Integer> customerIdsRDD = groupedDF
    .select("customer_id")
    .distinct()
    .toJavaRDD()
    .map(row -> row.getInt(0));

    List<Integer> idList = customerIdsRDD.collect();

    // System.out.println(idList);

    JavaRDD<String> customerValueRDD = groupedDF
    .select("movies_and_ratings")
    .toJavaRDD()
    .map(row -> row.getString(0));

    List<String> valuesList = customerValueRDD.collect();


    // System.out.println(valuesList);

    for(int i = 0; i < idList.size(); i++){
      mp.put(idList.get(i),valuesList.get(i));
    }


    userCombinations = userCombinations.withColumn("user_u", userCombinations.col("user_u").cast(DataTypes.IntegerType));
    userCombinations = userCombinations.withColumn("user_v", userCombinations.col("user_v").cast(DataTypes.IntegerType));

    // Define the UDF
    UDF2<Integer, Integer, Double> myFunction = (UDF2<Integer, Integer, Double>) (value1, value2) -> {
        String val1 = mp.get(value1);
        String val2 = mp.get(value2);

        ArrayList<Double> lst1 = new ArrayList<>(); 
        ArrayList<Double> lst2 = new ArrayList<>(); 

        Pattern pattern = Pattern.compile("\\{(\\d+),\\s*(-?\\d+\\.?\\d*)}");
        Matcher matcher = pattern.matcher(val1);

        // Create a HashMap to store the key-value pairs
        HashMap<Integer, Double> hashMap = new HashMap<>();

        // Iterate through the matches and populate the HashMap
        while (matcher.find()) {
            int key = Integer.parseInt(matcher.group(1));
            double value = Double.parseDouble(matcher.group(2));
            hashMap.put(key, value);
        }

        Matcher matcher1 = pattern.matcher(val2);

        // Create a HashMap to store the key-value pairs
        HashMap<Integer, Double> hashMap2 = new HashMap<>();

        // Iterate through the matches and populate the HashMap
        while (matcher1.find()) {
            int key = Integer.parseInt(matcher1.group(1));
            double value = Double.parseDouble(matcher1.group(2));
            hashMap2.put(key, value);
        }
        
        for (Integer key : hashMap2.keySet()) {
          if (!hashMap.containsKey(key)) {
              lst2.add(hashMap2.get(key));
              lst1.add(0.0);
          }
          else if(hashMap.containsKey(key)){
            lst2.add(hashMap2.get(key));
            lst1.add(hashMap.get(key));
          }
        }
        for (Integer key : hashMap.keySet()) {
          if (!hashMap2.containsKey(key)) {
              lst1.add(hashMap.get(key));
              lst2.add(0.0);
          }
        }
        
                        // System.out.println(lst1);

                        // System.out.println(lst2);

        double dotProduct = calculateDotProduct(lst1, lst2);
        double norm1 = calculateNorm(lst1);
        double norm2 = calculateNorm(lst2);

        // System.out.println(dotProduct);
        // System.out.println(norm1);
        // System.out.println(norm2);
        //         System.out.println("----------------------------------------");


        // System.out.println(dotProduct/(norm1*norm2));
        return dotProduct/(norm1*norm2);
    };

    // Create a UserDefinedFunction from the UDF
    UserDefinedFunction myFunctionUdf = udf(myFunction, DataTypes.DoubleType);

    // Apply the UDF to user_u and user_v columns and create a new column "colA"
    userCombinations = userCombinations.withColumn("sim", myFunctionUdf.apply(col("user_u"), col("user_v")));
    userCombinations = userCombinations.na().fill(0);


    // System.out.println(mp);
    userCombinations.show();
    // joinedRating.show();

    //Calculate recommended movie for closest neighbor 

    // Set the values of K and M
    int K = 11;  // Number of nearest neighbors
    int M = 3; // Number of movies to recommend

    StructType schema = new StructType()
    .add("customer_id", DataTypes.StringType, false)
    .add("movies_id", DataTypes.createArrayType(DataTypes.IntegerType), false);

    //Kth nearest by ID
    Dataset<Row> RecMovies = spark.createDataFrame(List.of(), schema);
    // RecMovies.show();
    // System.out.println(idList);
    // System.out.println("joinedRaing");
    // joinedRating.show();
    for(int i = 0; i < idList.size(); i++){
      int userId = idList.get(i);
      // System.out.println(userId);
      Dataset<Row> nearestNeighbors = getKNearestNeighbors(spark, joinedRating, userId, K);
      // System.out.println("nearestNeighbors");
      // nearestNeighbors.show();
      Dataset<Row> movie = getMovie(nearestNeighbors, userId, M);
      RecMovies = RecMovies.union(movie);

      // movie.show();

    }


    System.out.println("Movie recs for K nearest");
    RecMovies.show();


    // groupedDF.show();
    spark.stop();


	}
  private static Dataset<Row> getMovie(Dataset<Row> nearestNeighbors, int userId, int M){
    Dataset<Row> topMovies = nearestNeighbors.groupBy("movie_id")
    .agg(avg("rating").as("average_rating"));

    topMovies = topMovies.orderBy(col("average_rating").desc());

    topMovies = topMovies
    .limit(M)
    .select("movie_id");
    
    return topMovies
    .withColumn("customer_id", lit("customer_id_" + userId))
    .groupBy("customer_id")
    .agg(sort_array(collect_list(col("movie_id"))).as("movies_id"));

  }

  private static Dataset<Row> getKNearestNeighbors(SparkSession spark, Dataset<Row> userCombinations, int userId, int K){

    Dataset<Row> movie = userCombinations.filter(col("customer_id").equalTo(userId)).select("movie_id");

    userCombinations = userCombinations.withColumn("customer_id", userCombinations.col("customer_id").cast(DataTypes.IntegerType));

    Dataset<Row> filteredDFxx = userCombinations.filter(col("customer_id").notEqual(userId));

    filteredDFxx = filteredDFxx.select("customer_id").distinct();

    // filteredDFxx.show();
    // Order by the absolute difference between customer_id and the targetCustomerId
    filteredDFxx = filteredDFxx
            .withColumn("abs_diff", abs(col("customer_id").minus(userId)))
            .orderBy("abs_diff")
            .limit(K);

    // // Collect the result into a List<Integer>
    // List<Integer> lst = filteredDFxx.select("customer_id")
    // .as(Encoders.INT())
    // .collectAsList();
    
    // // userCombinations.show();
    // // System.out.println(lst); 

    // Dataset<Row> customerIDsDF = spark.createDataset(lst, Encoders.INT())
    // .toDF("customer_id_to_filter");

    // System.out.println("filteredDFxx");
    // filteredDFxx.show();
    // Join the original DataFrame with the customer IDs DataFrame to filter rows
    Dataset<Row> filteredDF = userCombinations
    .where(userCombinations.col("customer_id").equalTo(filteredDFxx.col("customer_id")))
    .select(userCombinations.col("*"));

    return filteredDF.join(movie, filteredDF.col("movie_id").equalTo(movie.col("movie_id")), "left_anti");

  } 

  private static double calculateDotProduct(List<Double> list1, List<Double> list2) {
    if (list1.size() != list2.size()) {
        throw new IllegalArgumentException("ArrayLists must have the same size");
    }

    double result = 0.0;

    for (int i = 0; i < list1.size(); i++) {
        result += list1.get(i) * list2.get(i);
    }

    return result;
  }
  private static double calculateNorm(List<Double> list) {
    double sumOfSquares = 0.0;

    for (Double value : list) {
        sumOfSquares += Math.pow(value, 2);
    }

    return Math.sqrt(sumOfSquares);
  }



}
