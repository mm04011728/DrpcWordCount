package mm12432.github.io.storm;

import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;





public class DrpcWordCountTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Config conf = new Config();
        conf.setDebug(false);
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);
		
		DRPCClient client = new DRPCClient(conf,"hadoop", 3772); 
		String [] strargs=new String[]{"hello","world","word","count","storm","spark","machine learning","hadoop","bigdata"};
		Random random=new Random();
		for (int i = 0; i < 1000; i++) {
			String result = client.execute("drpcwordcount", strargs[random.nextInt(strargs.length)]);
			System.out.println(result);
			Thread.sleep(100);
		}
		
	}

}
