package com.es.storm.words;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordsBoltD extends BaseRichBolt {
    private       Logger               log      = LoggerFactory.getLogger(this.getClass());
    private       Map                  stormConf;
    private       TopologyContext      context;
    private       OutputCollector      collector;
    private final Map<String, Integer> totalMap = new LinkedHashMap<>();
    private final Random               r        = new Random();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
        log.info("{} prepare.", this.getClass().getName());
        totalMap.put("Apple", 0);
        totalMap.put("Grape", 0);
        totalMap.put("Guava", 0);
    }

    @Override
    public void execute(Tuple input) {
        String fruit = input.getStringByField("fruit");
        int total = input.getIntegerByField("total");
        totalMap.put(fruit, totalMap.get(fruit) + total);
//        log.info("{}", totalMap);
        if (r.nextBoolean()) {
            collector.ack(input);
            log.info("Send ack {}", input);
        } else {
            collector.fail(input);
            log.info("Send fail {}", input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
