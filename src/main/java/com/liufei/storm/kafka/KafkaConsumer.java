package com.liufei.storm.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer {
    private String topic;
    private ConsumerConnector consumerConnector;

    public KafkaConsumer(String topic) {
        this.topic = topic;
        startkafkaConsumer();
    }

    private void startkafkaConsumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.1.107:2181,192.168.1.108:2181,192.168.1.109:2181");
        props.put("group.id", "eshop-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = consumerMap.get(topic);
        for (KafkaStream kafkaStream:kafkaStreams) {
            new Thread(new KafkaMessageProcessor(kafkaStream)).start();
        }
    }
}
