package com.virtualpairprogrammers.sparkSql;

import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.reflect.internal.pickling.UnPickler;

import static org.apache.spark.sql.functions.*;

public class section2930JavaAPIver {

    public static void main(String[] args) {

        // SparkSQL의 DataSet 사용

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        // 파티션 개수 제한 안해놀으면 아래 예제에서 약 200개 가량의 job이 만들어짐
        // 로컬 컴퓨터의 코어 개수 4개 -> 실질적으론 4개 job만 병렬적으로 돌아감, 대부분은 idle 상태 -> 성능 저하
        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");



        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType)
        );

        dataset = dataset.groupBy("level", "month", "monthnum")
                .count().as("total").orderBy("monthnum");

        dataset = dataset.drop("monthnum");

        dataset.show(100);
        dataset.explain();
//
        Scanner sc = new Scanner(System.in);
        sc.nextLine();

        spark.stop();

        /*
        Section 30

        explain : 실행 계획
        ->
        1. SQL Version
        - SortAggregate를 사용함.

        2. JAVA Api VERSION
        - HashAggregate를 사용함.

        SparkSQL can use two algorithms for grouping

        a) SortAggregate
         sort the rows and then gather together the matching rows
         -> O(nlogn) : not good at massive dataset
         -> do not particularly need lots of extra memory to implements it.
         (memory efficient, time inefficient)

        b) HashAggregate
          - group by 컬럼들로 Key, Value : 집계값인 Hash map 생성
          - 행 순회하면서 Key가 hash map에 존재하지 않으면 값 추가, 존재하면 집계값 업데이트..
          -> O(n)
          (time efficient, memory inefficient)

        SparkSQL will use HashAggregate Where possible...

        -> ** When is it Possible ? **
        HashAggregation is only possible. if the data for the "value" is MUTABLE.
        (Integer, float, boolean etc.., String : not MUTABLE)

        The "HashMap" is not a HashMap.. ???
        -> It is implemented in native memory. (not jvm virtual memory)
        -> there's no garbage collection going on here

         */

        /*
            결론 : HashAggregation을 사용하도록 sql을 적절히 바꾸면,
            sql vs JAVA API 성능 간 차이는 별로 없다.

         */


    }
}
