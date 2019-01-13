package com.liufei.storm;

import com.liufei.storm.bolt.CountProductBolt;
import com.liufei.storm.bolt.GetProductIdBolt;
import com.liufei.storm.spout.HotProductSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class StormTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("HotProductSpout",new HotProductSpout(),1);
        topologyBuilder.setBolt("GetProductIdBolt",new GetProductIdBolt(),5).setNumTasks(5).shuffleGrouping("HotProductSpout");
        topologyBuilder.setBolt("CountProductBolt",new CountProductBolt(),5).setNumTasks(5).fieldsGrouping("GetProductIdBolt",new Fields("productId"));

        Config config = new Config();

        if(args!=null&&args.length>0){
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("StormTopology",config,topologyBuilder.createTopology());
            Utils.sleep(30000);
            localCluster.shutdown();
        }
    }
}
