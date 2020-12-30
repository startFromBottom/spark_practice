package com.virtualpairprogrammers.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class Section2930SQLver {

    public static void main(String[] args) {

        // SparkSQL의 SQL 문법 사용

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        // 파티션 개수 제한 안해놀으면 아래 예제에서 약ㄱ 200개 가량의 job이 만들어짐
        // 로컬 컴퓨터의 코어 개수 4개 -> 실질적으론 4개 job만 병렬적으로 돌아감, 대부분은 idle 상태 -> 성능 저하
        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql(
                "select level, date_format(datetime, 'MMMM') as month, count(1) as total, " +
                        "first(date_format(datetime, 'M')) as monthnum " + // monthnum을 int로 형변환하면, hashaggregate 사용이 가능하다!
                        "from logging_table " +
                        "group by level, month " +
                        "order by monthnum, level"
        );
//
        results = results.drop("monthnum");
        results.explain();
        results.show(100);

        Scanner sc = new Scanner(System.in);
        sc.nextLine();

        spark.stop();


    }

}
