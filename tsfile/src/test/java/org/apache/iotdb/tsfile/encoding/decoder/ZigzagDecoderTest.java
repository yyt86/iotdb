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

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.ZigzagEncoder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Deprecated
public class ZigzagDecoderTest {

  private static final Logger logger = LoggerFactory.getLogger(BitmapDecoderTest.class);

  private List<Integer> intList;
  private List<Boolean> booleanList;

  @Before
  public void setUp() {
    intList = new ArrayList<Integer>();
    int[] int_array = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int int_len = int_array.length;
    int int_num = 100000;
    for (int i = 0; i < int_num; i++) {
      intList.add(int_array[i % int_len]);
    }

    booleanList = new ArrayList<Boolean>();
    boolean[] boolean_array = {true, false, true, true, false, true, false, false};
    int boolean_len = boolean_array.length;
    int boolean_num = 100000;
    for (int i = 0; i < boolean_num; i++) {
      booleanList.add(boolean_array[i % boolean_len]);
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testZigzagReadInt() throws Exception {
      float sum_ratio = 0;
      float sum_throughput =0;
    for (int i = 1; i < 10; i++) {
      float[] ret = testInt(intList, false, i);
        sum_ratio += ret[0];
        sum_throughput += ret[1];
    }
    System.out.println("average ratio " + sum_ratio/10 + " average throughput " + sum_throughput/10);
  }

  private float[] testInt(List<Integer> list, boolean isDebug, int repeatCount) throws Exception {
      float[] ret = new float[2];
    long start = System.currentTimeMillis();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ZigzagEncoder encoder = new ZigzagEncoder();
//    String filePath = new File("").getAbsolutePath();
//    System.out.println (filePath);

    String file = "/Users/yuting/Documents/openSource/iotdb/tsfile/src/test/java/org/apache/iotdb/tsfile/encoding/decoder/linear.csv";
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

//    for (int i = 0; i < repeatCount; i++) {
//      for (int value : list) {
//        encoder.encode(value, baos);
//      }
//
//      encoder.flush(baos);
//    }
    int value_;
    ByteBuffer bais = ByteBuffer.wrap(baos.toByteArray());
    Decoder decoder = new ZigzagDecoder();
    value_ = decoder.readInt(bais);
//    for (int i = 0; i < repeatCount; i++) {
//      for (int value : list) {
//        int value_ = decoder.readInt(bais);
//        if (isDebug) {
//          logger.debug("{} // {}", value_, value);
//        }
//        assertEquals(value, value_);
//      }
//    }

    long end = System.currentTimeMillis();
    long time = end - start;

    System.out.println("time " + time  + " " + (float)size/time/1000);
    ret[0] = encoder.ratio;
    ret[1] = (float)size/time/1000;
    return ret;
  }
}
