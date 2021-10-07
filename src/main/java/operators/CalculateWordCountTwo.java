package operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateWordCountTwo extends KeyedCoProcessFunction<Tuple,Tuple2<String, String>,Tuple2<String, String>, Tuple2<String, Integer>>{

	
	private static final Logger logger = LoggerFactory.getLogger(CalculateWordCountTwo.class);
	private static final long serialVersionUID = 1L;
	
	private ValueState<Integer> state;
	@Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Integer.class));
    }
	@Override
	public void processElement1(Tuple2<String, String> input1,
			Context ctx,
			Collector<Tuple2<String, Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		int count=0;
		try {
			count=state.value();
		}
		catch(Exception e){
			state.update(0);
		}
		logger.warn("initialstate-part1 {} {}",input1.f0,count);
		count=count+1;
		state.update(count);
		out.collect(Tuple2.of(input1.f0, count));
	}

	@Override
	public void processElement2(Tuple2<String, String> input2,
			Context ctx,
			Collector<Tuple2<String, Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		
		int count=0;
		try {
			count=state.value();
		}
		catch(Exception e){
			state.update(0);
		}
		logger.warn("initialstate in part2 {} {}",input2.f0,count);
		count=count-1;
		state.update(count);
		out.collect(Tuple2.of(input2.f0, count));
		
	}

}
