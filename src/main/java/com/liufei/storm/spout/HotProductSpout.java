package com.liufei.storm.spout;

import com.liufei.storm.kafka.KafkaConsumer;
import com.liufei.storm.queue.HotProductQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.Map;

public class HotProductSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    HotProductQueue hotProductQueue = HotProductQueue.getInstance();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        new KafkaConsumer("access");
    }

    @Override
    public void nextTuple() {
        while (true){
            if (hotProductQueue.size()>0){
                try {
                    String hotProduct = hotProductQueue.take();
                    spoutOutputCollector.emit(new Values(hotProduct));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                Utils.sleep(200);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hotProduct"));
    }
}
