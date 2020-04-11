package com.cmbc.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;


public class SQLBatch {
    public static void main(String[] args) throws Exception {
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        // read files
        DataSet<String> s_students = env.readTextFile("/Users/guoxingyu/Documents/work/java/flink/flinksql/src/main/resources/students.txt");
        DataSet<String> s_score = env.readTextFile("/Users/guoxingyu/Documents/work/java/flink/flinksql/src/main/resources/scores.txt");

        // prepare data
        DataSet<Tuple3<Integer, String, String>> students = s_students.map(new MapFunction<String, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> map(String s) throws Exception {
                String[] line = s.split(" ");
                return new Tuple3<Integer, String, String>(Integer.valueOf(line[0]), line[1], line[2]);
            }
        });

        DataSet<Tuple4<Integer, Integer, Integer, Integer>> score = s_score.map(new MapFunction<String, Tuple4<Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple4<Integer, Integer, Integer, Integer> map(String s) throws Exception {
                String[] line = s.split(" ");
                return new Tuple4<Integer, Integer, Integer, Integer>(Integer.valueOf(line[0]), Integer.valueOf(line[1]),
                        Integer.valueOf(line[2]), Integer.valueOf(line[3]));
            }
        });

        // join data
        DataSet<Tuple6<Integer, String, String, Integer, Integer, Integer>> data = students.join(score)
                .where(0)
                .equalTo(0)
                .projectFirst(0,1,2)
                .projectSecond(1,2,3);


        // register to a table
        tEnv.registerDataSet("Data", data, "id, name, classname, chinese, math, english");


        // do sql
        Table sqlQuery = tEnv.sqlQuery("SELECT classname, AVG(chinese) as avg_chinese, AVG(math) as avg_math, AVG(english) as avg_english, " +
                "AVG(chinese + math + english) as avg_total " +
                "FROM Data " +
                "GROUP BY classname " +
                "ORDER BY avg_total"
        );

        // to sink
        DataSet<Info> result = tEnv.toDataSet(sqlQuery, Info.class);
        result.writeAsText("/Users/guoxingyu/Documents/work/java/flink/flinksql/src/main/resources/info.txt", OVERWRITE);
        tEnv.execute("do flink sql demo in batch");

    }

    public static class Info {
        public String classname;
        public Integer avg_chinese;
        public Integer avg_math;
        public Integer avg_english;
        public Integer avg_total;

        public Info() {
        }

        public Info(String classname, Integer avg_chinese, Integer avg_math, Integer avg_english, Integer avg_total) {
            this.classname = classname;
            this.avg_chinese = avg_chinese;
            this.avg_math = avg_math;
            this.avg_english = avg_english;
            this.avg_total = avg_total;
        }

        @Override
        public String toString() {
            return
                    "classname=" + classname +
                    ", avg_chinese=" + avg_chinese +
                    ", avg_math=" + avg_math +
                    ", avg_english=" + avg_english +
                    ", avg_total=" + avg_total +
                    "";
        }
    }
}
