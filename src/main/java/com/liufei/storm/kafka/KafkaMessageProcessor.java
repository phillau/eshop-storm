package com.liufei.storm.kafka;

import com.liufei.storm.queue.HotProductQueue;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaMessageProcessor implements Runnable {

    private KafkaStream kafkaStream;

    public KafkaMessageProcessor(KafkaStream kafkaStream) {
        this.kafkaStream = kafkaStream;
    }

    @Override
    public void run() {
        HotProductQueue hotProductQueue = HotProductQueue.getInstance();
        ConsumerIterator<byte[],byte[]> iterator = kafkaStream.iterator();
        while (iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("message="+message);
            try {
                hotProductQueue.put(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
