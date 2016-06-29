package mm12432.github.io.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;



public class DrpcWordCountTopology {

	@SuppressWarnings("serial")
	public static class CountBolt extends BaseBasicBolt {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String input = tuple.getString(1);
			Integer count = counts.get(input);
			if (count == null)
				count = 0;
			count++;
			counts.put(input, count);
			collector.emit(new Values(tuple.getValue(0), input + ":" + count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}

	}
	
	@SuppressWarnings("serial")
	public static class GetWordBlot extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			collector.emit(new Values(input.getValue(0), input.getValue(1)));
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "word"));
			
		}
		
	}
	public static void main(String[] args) throws Exception {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("drpcwordcount");
		builder.addBolt(new GetWordBlot(),3).shuffleGrouping();
		builder.addBolt(new CountBolt(), 3).fieldsGrouping(new Fields("word"));

		Config conf = new Config();

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
		}

	}

}
