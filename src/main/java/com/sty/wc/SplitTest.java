package com.sty.wc;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        SplitStream<Integer> split = streamSource.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer integer) {
                ArrayList<String> list = new ArrayList<>();
                list.add(integer % 2 == 0 ? "even" : "odd");
                return list;
            }
        });
        split.select("even").print();

        env.execute();
    }
}
