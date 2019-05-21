package com.es.storm.words;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordsDataSpout extends BaseRichSpout {

    private       Logger               log    = LoggerFactory.getLogger(this.getClass());
    private       Map                  conf;
    private       TopologyContext      context;
    private       SpoutOutputCollector collector;
    private final Random               r      = new Random();
    private       String[]             fruits = new String[]{"Apple", "Grape", "Guava"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        log.info("{} open.", this.getClass().getName());
    }

    @Override
    public void nextTuple() {
        String fruit = fruits[r.nextInt(fruits.length)];
        int quantity = r.nextInt(5) + 1;
        Values values = new Values(fruit, quantity);
        collector.emit("WordA", values);
        collector.emit("WordB", values);
        collector.emit("WordC", values);
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("fruit={}, quantity={}", fruit, quantity);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("WordA", new Fields("fruit", "quantity"));
        declarer.declareStream("WordB", new Fields("fruit", "quantity"));
        declarer.declareStream("WordC", new Fields("fruit", "quantity"));
    }
}
