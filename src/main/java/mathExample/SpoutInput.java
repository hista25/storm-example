package mathExample;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class SpoutInput extends BaseRichSpout 
{
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	int counter = 0;
	@Override
	public void fail(Object id) 
	{
		    
	}
	
	@Override
	public void ack(Object id) 
	{
	  
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
	    declarer.declare(new Fields("sentence"));
	}

	public void nextTuple() 
	{
		Utils.sleep(100);
	    String [] example = {"2 4","s 6","5 7"};
	    if(counter > example.length-1) return;
	    String character = example[counter];
	    
	    System.out.println("Emmittinggggg: "+ character);	    
	    _collector.emit(new Values(character));
	    counter++;
		
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) 
	{
		_collector = collector;
		_rand = new Random();
	}
	  
}