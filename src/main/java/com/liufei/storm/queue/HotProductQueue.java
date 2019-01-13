package com.liufei.storm.queue;

import java.util.concurrent.ArrayBlockingQueue;

public class HotProductQueue extends ArrayBlockingQueue<String>{

    private HotProductQueue(int capacity) {
        super(capacity);
    }

    private static class InnerHotProductQueue{
        private static HotProductQueue hotProductQueue = new HotProductQueue(1000);
    }

    public static HotProductQueue getInstance(){
        return InnerHotProductQueue.hotProductQueue;
    }
}
