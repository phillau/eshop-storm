package com.liufei.storm.bolt;

import org.apache.commons.collections.map.LRUMap;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

public class CountProductBolt extends BaseRichBolt {
    private LRUMap lruMap = new LRUMap();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        String productId = tuple.getStringByField("productId");
        Integer count = (Integer) lruMap.get(productId);
        lruMap.put(productId, StringUtils.isBlank(productId) ? (count = 0) : (++count));
        System.err.println("productId为【"+productId+"】的商品热度为" + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
