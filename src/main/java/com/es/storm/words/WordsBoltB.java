package com.es.storm.words;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordsBoltB extends BaseRichBolt {
    private Logger          log = LoggerFactory.getLogger(this.getClass());
    private Map             stormConf;
    private TopologyContext context;
    private OutputCollector collector;
    private int             total;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
        log.info("{} prepare.", this.getClass().getName());
    }

    @Override
    public void execute(Tuple input) {
        String fruit = input.getStringByField("fruit");
        if (!"Grape".equals(fruit)) {
            collector.ack(input);
            return;
        }
        int quantity = input.getIntegerByField("quantity");
        total += quantity;
        collector.emit("FruitCount", input, new Values("Grape", total));
        collector.ack(input);
//        log.info("fruit={}, total={}", fruit, total);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("FruitCount", new Fields("fruit", "total"));
    }
}
