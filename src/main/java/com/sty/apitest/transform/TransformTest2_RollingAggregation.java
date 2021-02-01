package com.sty.apitest.transform;

import com.sty.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("/Users/sty/IdeaProjects/flink-test/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = streamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] splits = s.split(",");
                SensorReading sensorReading = new SensorReading(splits[0], new Long(splits[1]), new Double(splits[2]));
                return sensorReading;
            }
        });
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");
        SingleOutputStreamOperator<SensorReading> resultStream1 = keyedStream.max("temperature");


        resultStream.print();

        env.execute();

    }
}
