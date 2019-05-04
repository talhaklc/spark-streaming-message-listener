package com.udemy.spark.streaming.message;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\Users\\talhaklc\\Desktop\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession=SparkSession.builder().appName("Message-Listener").master("local").getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream().format("socket").option("host", "localhost").option("port", "8005").load();

        Dataset<String> data = rawData.as(Encoders.STRING());

        Dataset<String> wordList = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> groupedData = wordList.groupBy("value").count();

        StreamingQuery start = groupedData.writeStream().outputMode("complete").format("console").start();

        start.awaitTermination();
    }
}
