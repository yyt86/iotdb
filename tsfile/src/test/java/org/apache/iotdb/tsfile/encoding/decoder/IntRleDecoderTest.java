/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.RleEncoder;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IntRleDecoderTest {

  private List<Integer> rleList;
  private List<Integer> bpList;
  private List<Integer> hybridList;

  @Before
  public void setUp() {
    rleList = new ArrayList<>();
    int rleCount = 11;
    int rleNum = 18;
    int rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        rleList.add(rleStart - 1);
      }
      rleCount += 2;
      rleStart *= -3;
    }

    bpList = new ArrayList<>();
    int bpCount = 100000;
    int bpStart = 11;
    for (int i = 0; i < bpCount; i++) {
      bpStart += 3;
      if (i % 2 == 1) {
        bpList.add(bpStart * -1);
      } else {
        bpList.add(bpStart);
      }
    }

    hybridList = new ArrayList<>();
    int hybridCount = 11;
    int hybridNum = 1000;
    int hybridStart = 20;

    for (int i = 0; i < hybridNum; i++) {
      for (int j = 0; j < hybridCount; j++) {
        hybridStart += 3;
        if (j % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      for (int j = 0; j < hybridCount; j++) {
        if (i % 2 == 1) {
          hybridList.add(hybridStart * -1);
        } else {
          hybridList.add(hybridStart);
        }
      }
      hybridCount += 2;
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testRleReadBigInt() throws IOException {
    List<Integer> list = new ArrayList<>();
    for (int i = 7000000; i < 10000000; i++) {
      list.add(i);
    }
    testLength(list, false, 1);
    for (int i = 1; i < 10; i++) {
      testLength(list, false, i);
    }
  }

  @Test
  public void testRleReadInt() throws IOException {
//    for (int i = 1; i < 10; i++) {
//      testLength(rleList, false, i);
//    }
    float sum_ratio = 0;
    float sum_throughput =0;
    for (int i = 1; i < 10; i++) {
      float[] ret = testLength(rleList, false, i);;
      sum_ratio += ret[0];
      sum_throughput += ret[1];
    }
    System.out.println("average ratio " + sum_ratio/10 + " average throughput " + sum_throughput/10);
  }

  @Test
  public void testMaxRLERepeatNUM() throws IOException {
    List<Integer> repeatList = new ArrayList<>();
    int rleCount = 17;
    int rleNum = 5;
    int rleStart = 11;
    for (int i = 0; i < rleNum; i++) {
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart);
      }
      for (int j = 0; j < rleCount; j++) {
        repeatList.add(rleStart / 3);
      }
      rleCount *= 7;
      rleStart *= -3;
    }
    for (int i = 1; i < 10; i++) {
      testLength(repeatList, false, i);
    }
  }

  @Test
  public void testBitPackingReadInt() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(bpList, false, i);
    }
  }

  @Test
  public void testHybridReadInt() throws IOException {
    for (int i = 1; i < 3; i++) {
      testLength(hybridList, false, i);
    }
  }

  @Test
  public void testHybridReadBoolean() throws IOException {
    for (int i = 1; i < 10; i++) {
      testLength(hybridList, false, i);
    }
  }

  @Test
  public void testBitPackingReadHeader() throws IOException {
    for (int i = 1; i < 505; i++) {
      testBitPackedReadHeader(i);
    }
  }

  public float[] testLength(List<Integer> list, boolean isDebug, int repeatCount) throws IOException {
    float[] ret = new float[2];
    long start = System.currentTimeMillis();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RleEncoder<Integer> encoder = new IntRleEncoder();
//    for (int i = 0; i < repeatCount; i++) {
//      for (int value : list) {
//        encoder.encode(value, baos);
//      }
//      encoder.flush(baos);
//    }
    String file = "/Users/yuting/Documents/openSource/iotdb/tsfile/src/test/java/org/apache/iotdb/tsfile/encoding/decoder/sin.csv";
    File target = new File(file);
    long size = target.length();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line = null;
    int value;
    int j = 0;
    while (true) {
      j = 0;
      while (j < Integer.MAX_VALUE && (line = br.readLine()) != null ) {
        value = Integer.valueOf(line.trim());
        encoder.encode(value, baos);
        j++;
      }
      encoder.flush(baos);
      if (line == null) break;
    }


    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    RleDecoder decoder = new IntRleDecoder();
//    for (int i = 0; i < repeatCount; i++) {
//      for (int value : list) {
//        int value_ = decoder.readInt(buffer);
//        if (isDebug) {
//          System.out.println(value_ + "/" + value);
//        }
//        assertEquals(value, value_);
//      }
//    }
    long end = System.currentTimeMillis();
    long time = end - start;
    int value_;
    value_ = decoder.readInt(buffer);
    System.out.println("time " + time  + " " + (float)size/time/1000);
    ret[0] = (float)encoder.cache_length / j /4;
    ret[1] = (float)size/time/1000*8;
    return ret;
  }

  private void testBitPackedReadHeader(int num) throws IOException {
    List<Integer> list = new ArrayList<>();

    for (int i = 0; i < num; i++) {
      list.add(i);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(list);
    RleEncoder<Integer> encoder = new IntRleEncoder();
    for (int value : list) {
      encoder.encode(value, baos);
    }
    encoder.flush(baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    assertEquals(bitWidth, bais.read());
    int header = ReadWriteForEncodingUtils.readUnsignedVarInt(bais);
    int group = header >> 1;
    assertEquals(group, (num + 7) / 8);
    int lastBitPackedNum = bais.read();
    if (num % 8 == 0) {
      assertEquals(lastBitPackedNum, 8);
    } else {
      assertEquals(lastBitPackedNum, num % 8);
    }
  }
}
