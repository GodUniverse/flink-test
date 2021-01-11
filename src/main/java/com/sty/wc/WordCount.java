package com.sty.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/Users/sty/IdeaProjects/flink-test/src/main/resources/word.txt";
        DataSet<String> source = env.readTextFile(inputPath);
        source.flatMap(new MyflatMapper())
                .groupBy(0)
                .sum(1)
                .print();
    }

    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
