package com.airing.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFSTest {

    private Configuration conf;
    private FileSystem fs;

    @Before
    public void connect() throws IOException {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @Test
    public void test() throws IOException {
        Path path = new Path("/airing");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    @After
    public void close() {

    }
}
