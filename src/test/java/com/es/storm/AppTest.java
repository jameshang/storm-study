package com.es.storm;

import org.junit.Assert;
import org.junit.Test;

public class AppTest {
    @Test
    public void testPing() {
        App app = new App();
        Assert.assertTrue(app.ping());
    }
}
