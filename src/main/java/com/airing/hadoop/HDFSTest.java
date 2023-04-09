package com.airing.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

    @Test
    public void upload() throws Exception {
        FileInputStream fileInputStream = new FileInputStream("./data/test.txt");

        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/airing/hello.txt"));

        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, conf, true);
    }

    @Test
    public void read() throws Exception {
        Path path = new Path("/airing/data.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            System.out.println(fileBlockLocation);
        }

        FSDataInputStream inputStream = fs.open(path);
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
        System.out.println((char) inputStream.readByte());
    }

    @After
    public void close() {

    }
}
