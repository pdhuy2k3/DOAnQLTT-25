package com.nhom25;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class Transformation {
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
                .appName("Statement Report")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("hive.execute.dynamic.partition", "true")
                .config("hive.execute.dynamic.partition.mode", "nonstrict")
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();

        // Load data to Spark DF
        var statementDF = spark.read().parquet("hdfs://localhost:9000/datalake/orders").drop("year", "month", "day");

        var preDF = statementDF.filter(col("created_at").equalTo(executionDate));

        // Aggregate data
        var mapDF = preDF.groupBy("credit", "bank")
                .agg(
                        sum("credit").as("total amount")
                );

        var resultDF = mapDF.withColumn("year", lit(year))
                                            .withColumn("month", lit(month))
                                            .withColumn("day", lit(day))
                                            .select("date", "no_or_code", "credit", "total amount", "notes", "name", "bank");

        resultDF.write()
                .format("hive")
                .partitionBy("year", "month", "day")
                .mode(SaveMode.Append)
                .saveAsTable("reports.total_statement");
    }
}