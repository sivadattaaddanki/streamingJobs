package allJobs;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import operators.CalculateWordCount;
import serializers.CustomDeserializerKeyedString;

public class Job1 {
	protected static String brokers="192.168.1.7:9092";

	public static void start() {
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "192.168.1.7:9092");

			@SuppressWarnings("deprecation")
			KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(brokers).setTopics("input_1")
					.setGroupId("my-group").setStartingOffsets(OffsetsInitializer.earliest())
					.setValueOnlyDeserializer(new SimpleStringSchema()).build();

			DataStream<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

			FlinkKafkaProducer<Tuple2<String, Integer>> myProducer = new FlinkKafkaProducer<Tuple2<String, Integer>>(
					"output_1", // target topic
					new CustomDeserializerKeyedString(), // serialization schema
					properties, // producer config
					FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

			@SuppressWarnings("deprecation")
			DataStream<Tuple2<String, String>> keyStream = input.map(new MapFunction<String, Tuple2<String, String>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> map(String value) throws Exception {
					// TODO Auto-generated method stub
					return Tuple2.of(Character.toString(value.charAt(0)), value);
				}

			}).name("keyStream");
			@SuppressWarnings("deprecation")
			DataStream<Tuple2<String, Integer>> output = keyStream.keyBy(0).process(new CalculateWordCount())
					.name("countStream");

			output.addSink(myProducer).name("sink");

			env.execute("job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	

}
