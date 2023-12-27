package com.RUSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
    SparkSession spark = SparkSession.builder().appName("NeflixMovieAverageJava").getOrCreate();

    Dataset<Row> df = spark.read().csv(InputPath);

    String[] newColumnNames = {"movie_id","customer_id","rating","date"};

    for (int i = 0; i < newColumnNames.length; i++) {
        df = df.withColumnRenamed(df.columns()[i], newColumnNames[i]);
    }

    df = df.withColumn("rating", df.col("rating").cast("double"));

    Dataset<Row> result = df.groupBy("movie_id")
    .agg(round(avg("rating"), 2).alias("avg_rating"));

    result = result.orderBy(col("avg_rating").desc());

    result.show();

    spark.stop();
		
	}

}
