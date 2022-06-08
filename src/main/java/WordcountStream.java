import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Description
 * @Author WANKAI
 * @Date 2021/12/18 14:47
 */

public class WordcountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "group_name");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // DataStream<String> inputDataStream = env.socketTextStream("localhost", 44245);

        DataStream<String> inputDataStream = env.addSource(new FlinkKafkaConsumer<>("wc_source_topic", new SimpleStringSchema(), props));

        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream.flatMap(new MyFlatMapper())
                //批处理:group by
                .keyBy(0).sum(1);

        wordCountDataStream.addSink(new FlinkKafkaProducer<>("localhost:9092", "wc_sink_topic", (SerializationSchema<Tuple2<String, Integer>>) msgDTO -> msgDTO.toString().getBytes(StandardCharsets.UTF_8)));

        // 2.2 把kafka接收debug打印出来
        wordCountDataStream.print().setParallelism(1).name("wc-job");

        env.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, org.apache.flink.util.Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
