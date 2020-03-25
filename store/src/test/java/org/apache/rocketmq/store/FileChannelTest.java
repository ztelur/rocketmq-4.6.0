/**
 * Superid.menkor.com Inc.
 * Copyright (c) 2012-2020 All Rights Reserved.
 */
package org.apache.rocketmq.store;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 *
 * @author libing
 * @version $Id: FileChannelTest.java, v 0.1 2020年01月21日 上午9:27 zt Exp $
 */

public class FileChannelTest {
    @Test
    public void testFileChannelAndByteBuffer() {
        FileChannel fileChannel = null;
        ByteBuffer writeBuffer = ByteBuffer.allocate(1024 * 1024);
        try {

            String data = "ssddsdfsdfsafsdfassfasfdasfdasf";

            fileChannel = new RandomAccessFile("/Users/gbc/Downloads/filechanneltest", "rw").getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, 1024 * 1024);
            writeBuffer.put(data.getBytes());
            mappedByteBuffer.put(data.getBytes());
            sleepForAWhile();
            mappedByteBuffer.force();
            sleepForAWhile();
            fileChannel.write(writeBuffer);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileChannel != null) {
                try {
                    fileChannel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void sleepForAWhile() {
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}