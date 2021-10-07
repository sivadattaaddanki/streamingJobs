package operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration;


public class CalculateWordCount extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Integer>> {

	/**
	 * 
	 */
	private static final Logger logger = LoggerFactory.getLogger(CalculateWordCount.class);
	private static final long serialVersionUID = 1L;
	
	private ValueState<Integer> state;
	@Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Integer.class));
    }

	@Override
	public void processElement(Tuple2<String, String> input,
			Context arg1,
			Collector<Tuple2<String, Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		int count=0;
		try {
			count=state.value();
		}
		catch(Exception e){
			state.update(0);
		}
		logger.warn("initialstate {} {}",input.f0,count);
		count=count+1;
		state.update(count);
		out.collect(Tuple2.of(input.f0, count));
	}
 
}
