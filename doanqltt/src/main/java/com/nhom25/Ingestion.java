package com.nhom25;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import static com.nhom25.Util.getCurrentDate;
import static com.nhom25.Util.parseArgs;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class Ingestion {
    public static void main(String[] args) throws IOException {
        Map<String, String> argMap = parseArgs(args);

        String tblName = argMap.get("-tblName");
        String executionDate = argMap.get("-execDate");
        if (tblName == null) {
            System.out.println("Missing required argument: -tblName");
            System.exit(1);
            return;
        }
        if (executionDate == null) {
            executionDate = getCurrentDate();
        }

        System.out.println("tblName: " + tblName);
        System.out.println("executionDate: " + executionDate);

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
//                .config("hive.metastore.warehouse.dir","hdfs://localhost:9000/warehouse/")
                .getOrCreate();

        // Get the latest record_id in DataLake
        var conf = spark.sparkContext().hadoopConfiguration();
        var fs = org.apache.hadoop.fs.FileSystem.newInstance(conf); // Sử dụng newInstance để tránh cache
        String path = String.format("/datalake/%s", tblName);
        var hdfsPath = new org.apache.hadoop.fs.Path(path);
        boolean exists = fs.exists(hdfsPath);  // Kiểm tra sự tồn tại

// Xác định đường dẫn trên HDFS
        var tblLocation = String.format("hdfs://192.168.1.188:9000/datalake/%s", tblName);
        Dataset<Row> jsonDF;

        if (exists) {
            System.out.println("Exists");
            var df = spark.read().parquet(tblLocation);
            var record_id = df.agg(max("no_or_code")).head().getLong(0);

            // Đọc JSON và lọc các bản ghi dựa trên record_id
            jsonDF = spark.read().json("doanqltt/src/main/resources/db/")
                    .filter("no_or_code > " + record_id);
        } else {
            System.out.println("Not Exists");
            // Đọc toàn bộ file JSON nếu bảng chưa tồn tại
            jsonDF = spark.read().json("doanqltt/src/main/resources/db/");
        }
        jsonDF = jsonDF
                .withColumn("name", trim(regexp_replace(col("name"), "\\u00e2\\u20ac\\u201c\\s*.*", "")))
                .withColumn("no_or_code", col("no_or_code").cast("long"))
                .withColumnRenamed("date", "transaction_date");
        UserDefinedFunction convertDateFormat = udf((String date) -> {
            try {
                if (date.contains("T")) {
                    // Handle "2024-09-12T21:10:16"
                    return new SimpleDateFormat("dd/MM/yyyy").format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(date));
                } else {
                    // Handle "08/09/2024"
                    return date; // Already in the desired format
                }
            } catch (Exception e) {
                return null; // Return null for any parsing error
            }
        }, DataTypes.StringType);
        var result= jsonDF.withColumn("transaction_date", convertDateFormat.apply(col("transaction_date")));
        // Save to DataLake
        var outPutDF = result
                .withColumn("year", lit(year))
                .withColumn("month", lit(month))
                .withColumn("day", lit(day));


        outPutDF.write().partitionBy("year", "month", "day").mode(SaveMode.Append).parquet(tblLocation);
        spark.stop();

    }

}
