package com.sty.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> streamSource1 = env.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2));
        DataStreamSource<Integer> streamSource2 = env.fromElements(1, 3, 5);

        ConnectedStreams<Tuple2<String, Integer>, Integer> connect = streamSource1.connect(streamSource2);
        connect.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer map1(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }

            @Override
            public Integer map2(Integer integer) throws Exception {
                return integer;
            }
        }).map(x -> x * 10).print();

        env.execute();
    }
}
