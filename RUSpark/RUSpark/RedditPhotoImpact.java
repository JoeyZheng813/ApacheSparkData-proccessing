package com.RUSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];

    // Create a Spark session
    SparkSession spark = SparkSession.builder().appName("RedditPhotoImpactJava").getOrCreate();

    Dataset<Row> df = spark.read().csv(InputPath);

    String[] newColumnNames = {"#image_id", "unixtime", "title", "subreddit", "number_of_upvotes", "number_of_downvotes", "number_of_comments"};

    for (int i = 0; i < newColumnNames.length; i++) {
        df = df.withColumnRenamed(df.columns()[i], newColumnNames[i]);
    }

    // Calculate the sum of upvotes and downvotes for each image_id
    Dataset<Row> result = df
            .groupBy("#image_id")
            .agg(sum("number_of_upvotes").alias("total_upvotes"), 
                 sum("number_of_downvotes").alias("total_downvotes"),
                 sum("number_of_comments").alias("total_comments"));

    Dataset<Row> finalResult = result
    .select(col("#image_id"),
            (col("total_upvotes").plus(col("total_downvotes")).plus(col("total_comments"))).alias("impact_score"));

    finalResult = finalResult.orderBy(col("impact_score").desc());

    finalResult.show();

    spark.stop();
		
	}

}
