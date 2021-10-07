package serializers;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
public class CustomDeserializerKeyedString implements  KafkaSerializationSchema<Tuple2<String,Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, Long timestamp) {
		// TODO Auto-generated method stub
		
		byte[] x=element.f0.getBytes();
		String topic="output_1";
		byte[] y =element.f1.toString().getBytes();
		return new ProducerRecord<>(topic,x,y);
	}
	

}
