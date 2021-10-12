package com.footlocker.sprinklr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataTransfer {

    public static void main(String [] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("Avro to parquet Data Transfer")
                .getOrCreate();

        try{
            /*<dependency>
             <groupId>com.databricks</groupId>
             <artifactId>spark-avro_2.11</artifactId>
             <version>4.0.0</version>
             </dependency>*/
            Dataset<Row> inputData = spark.read().format("com.databricks.spark.avro").load(inputPath);
            inputData.write().parquet(outputPath);
            spark.stop();
        } catch (Exception e) {
            spark.stop();
            e.printStackTrace();
            System.exit(1);
        }

    }
}
