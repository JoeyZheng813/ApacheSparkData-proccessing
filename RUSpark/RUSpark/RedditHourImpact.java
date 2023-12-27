package com.RUSpark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
    // Create a Spark session
    SparkSession spark = SparkSession.builder().appName("RedditHourImpactJava").getOrCreate();

    Dataset<Row> df = spark.read().csv(InputPath);

    String[] newColumnNames = {"#image_id", "unixtime", "title", "subreddit", "number_of_upvotes", "number_of_downvotes", "number_of_comments"};

    for (int i = 0; i < newColumnNames.length; i++) {
        df = df.withColumnRenamed(df.columns()[i], newColumnNames[i]);
    }

    df = df.withColumn("unixtime", col("unixtime").cast("Long"));
    UserDefinedFunction hourOfUnixUDF = udf((Long unixtime) -> hourOfUnix(unixtime), DataTypes.LongType);

    df = df.withColumn("hour", hourOfUnixUDF.apply(col("unixtime")).cast(DataTypes.LongType));

    Dataset<Row> allHours = spark.range(0, 24).toDF("hour");

    Dataset<Row> Hoursdf = allHours.join(
      df.groupBy("hour").agg(
        functions.sum("number_of_upvotes").as("total_upvotes"),
        functions.sum("number_of_downvotes").as("total_downvotes"),
        functions.sum("number_of_comments").as("total_comments")).withColumnRenamed("hour", "hour_group"), 
      col("hour").equalTo(col("hour_group")),"left_outer").orderBy("hour").na().fill(0);



    Dataset<Row> finalResult = Hoursdf
    .select(col("hour"),
            (col("total_upvotes").plus(col("total_downvotes")).plus(col("total_comments"))).alias(" impact_score")); 
    finalResult.show(24);

    spark.stop();
		
	}

  private static long hourOfUnix(long unixTime){
        Instant instant = Instant.ofEpochSecond(unixTime);
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("America/New_York"));
        String hr = zonedDateTime.format(DateTimeFormatter.ofPattern("HH"));
        return Long.parseLong(hr);
  }

}
