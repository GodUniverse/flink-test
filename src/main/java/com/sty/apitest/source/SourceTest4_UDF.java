package com.sty.apitest.source;

import com.sty.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.addSource(new MySensorSource()).print();

        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            Random random = new Random();
            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0; i < 2; i++) {
                map.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String id : map.keySet()) {
                    double newTemp = map.get(id) + random.nextGaussian();
                    map.put(id,newTemp);
                    sourceContext.collect(new SensorReading(id,System.currentTimeMillis(),newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
