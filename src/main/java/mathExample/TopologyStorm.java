package mathExample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyStorm {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new SpoutInput());
    builder.setBolt("add", new BoltAdd()).shuffleGrouping("spout");
    //builder.setBolt("count", new CountBoltStorm(), 12).fieldsGrouping("split", new Fields("word"));

    
    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(10);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("BoltAdd", conf, builder.createTopology());
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}