package com.sty.apitest.source;

import com.sty.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> streamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_3", 1547718200L, 15.4),
                new SensorReading("sensor_5", 1547718201L, 6.7),
                new SensorReading("sensor_7", 1547718202L, 38.1)
        ));
        streamSource.print();

        env.execute();
    }
}
