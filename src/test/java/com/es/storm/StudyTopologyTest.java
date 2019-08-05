package com.es.storm;

import org.junit.Test;

public class StudyTopologyTest {

    @Test
    public void testSubmitLocalTopology() throws Exception {
        StudyTopology studyTopology = new StudyTopology();
        studyTopology.submitLocalTopology("words");
        Thread.sleep(30000);
//        System.in.read();
    }

}
