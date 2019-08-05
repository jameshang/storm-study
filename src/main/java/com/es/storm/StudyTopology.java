package com.es.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.es.storm.words.WordsBoltA;
import com.es.storm.words.WordsBoltB;
import com.es.storm.words.WordsBoltC;
import com.es.storm.words.WordsBoltD;
import com.es.storm.words.WordsDataSpout;

public class StudyTopology {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private Config buildConfig() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);//设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，以防止tuple队列爆掉
        return config;
    }

    private StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        /*DGT*/

        builder.setSpout("wordsDataSpout", new WordsDataSpout(), 1);
        builder.setBolt("wordsBoltA", new WordsBoltA(), 1).shuffleGrouping("wordsDataSpout", "Fruit");
        builder.setBolt("wordsBoltB", new WordsBoltB(), 1).shuffleGrouping("wordsDataSpout", "Fruit");
        builder.setBolt("wordsBoltC", new WordsBoltC(), 1).shuffleGrouping("wordsDataSpout", "Fruit");
        builder.setBolt("wordsBoltD", new WordsBoltD(), 1)
                .shuffleGrouping("wordsBoltA", "FruitCount")
                .shuffleGrouping("wordsBoltB", "FruitCount")
                .shuffleGrouping("wordsBoltC", "FruitCount");
        return builder.createTopology();
    }

    public void submitTopology(String name) {
        try {
            Config config = buildConfig();
            StormTopology stormTopology = buildTopology();
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(name, config, stormTopology);
        } catch (Exception e) {
            log.error("Submit topology failed!", e);
        }
    }

    public void submitLocalTopology(String name) {
        try {
            Config config = buildConfig();
            StormTopology stormTopology = buildTopology();
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(name, config, stormTopology);
        } catch (Exception e) {
            log.error("Submit local topology failed!", e);
        }
    }
}
