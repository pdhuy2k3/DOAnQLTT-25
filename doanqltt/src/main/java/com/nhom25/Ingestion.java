package com.nhom25;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
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
                .withColumn("name", col("name").cast("string"))
                .withColumn("no_or_code", col("no_or_code").cast("long"))
                .withColumn("transaction_date", col("date"));

        // Save to DataLake
        var outPutDF = jsonDF

                .withColumn("year", lit(year))
                .withColumn("month", lit(month))
                .withColumn("day", lit(day));


        outPutDF.write().partitionBy("year", "month", "day").mode(SaveMode.Append).parquet(tblLocation);
        spark.stop();

    }

}
