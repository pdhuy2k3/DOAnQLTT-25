package com.nhom25;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.Scanner;

public class Ingestion {
    public static void main(String[] args) throws IOException {
        //Receive parameter from user input
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter your table name: ");
        String tblName = scanner.nextLine();

        System.out.print("Enter your execution date: ");
        String executionDate = scanner.nextLine();

        // Get information from user input
        var runTime = executionDate.split("-");
        var year = runTime[0];
        var month = runTime[1];
        var day = runTime[2];

        // Create Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Ingestion - From MySQL to HIVE")
                .master("local[*]")
                .getOrCreate();

        // Get the latest record_id in DataLake
        var conf = spark.sparkContext().hadoopConfiguration();
        var fs = org.apache.hadoop.fs.FileSystem.get(conf);
        String path = String.format("/datalake/%s", tblName);
        var exists = fs.exists(new org.apache.hadoop.fs.Path(path));
        var tblLocation = String.format("hdfs://localhost:9000/datalake/%s", tblName);
        Dataset<Row> jsonDF;

        if (exists) {
            var df = spark.read().parquet(tblLocation);
            var record_id = df.agg(max("no_or_code")).head().getLong(0);

            // Read JSON and filter records based on record_id
            jsonDF = spark.read().json("doanqltt/src/main/resources/db/")
                    .filter("no_or_code > " + record_id);
        } else {
            // Read the entire JSON file if the table doesn't exist
            jsonDF = spark.read().json("doanqltt/src/main/resources/db/");
        }
        jsonDF = jsonDF.withColumn("no_or_code", col("no_or_code").cast("long"));

        // Save to DataLake
        var outPutDF = jsonDF
                                    .withColumn("year", lit(year))
                                    .withColumn("month", lit(month))
                                    .withColumn("day", lit(day));

        outPutDF.write().partitionBy("year", "month", "day").mode(SaveMode.Append).parquet(tblLocation);
    }
}