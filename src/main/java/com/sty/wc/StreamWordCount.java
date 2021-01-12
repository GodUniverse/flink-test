package com.sty.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        String inputPath = "/Users/sty/IdeaProjects/flink-test/src/main/resources/word.txt";
//        DataStream<String> streamSource = env.readTextFile(inputPath);
        DataStream<String> streamSource = env.socketTextStream("localhost", 7777);
        streamSource.flatMap(new MyflatMapper())
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }

    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
