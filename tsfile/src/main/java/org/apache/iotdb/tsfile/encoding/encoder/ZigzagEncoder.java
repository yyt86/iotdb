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

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * An encoder implementing Zigzag encoding.
 *
 * <pre>Encoding format: {@code
 * <map> <indexes>
 * <map> := <map length> <map data>
 * <map data> := [<entry size><entry data>]...
 * <indexes> := [<index>]...
 * }</pre>
 */

public class ZigzagEncoder extends Encoder {
    private static final Logger logger = LoggerFactory.getLogger(DictionaryEncoder.class);
    private List<Integer> values;
    public ZigzagEncoder() {
        super(TSEncoding.ZIGZAG);
        this.values = new ArrayList<>();
        logger.debug("tsfile-encoding ZigzagEncoder: init zigzag encoder");
    }

    /**
     * transfer int to zigzag data
     */
    private int int_to_zigzag(int n){
        return (n <<1) ^ (n >>31);
    }

    /**
     * compression
     */
    private byte[] write_to_buffer(int n) {
        byte[] buf = new byte[5];
        int idx = 0;
        while (true) {
            if ((n & ~0x7F) == 0) {
                buf[idx++] = (byte)n;
                break;
            } else {
                buf[idx++] = (byte)((n & 0x7F) | 0x80);
                n >>>= 7;
            }
        }
        return Arrays.copyOfRange(buf, 0, idx);
    }

    /**
     * transfer zigzag data to int
     */
    private int zigzagToInt(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * decompression
     */
    private int read_from_buffer(byte[] buf){
        int ret = 0;
        int shift = 0;
        int offset = 0;
        while (true) {
            byte b = buf[offset];
            ret |= (int) (b & 0x7f) << shift;
            if ((b & 0x80) != 0x80) break;
            shift += 7;
        }
        return ret;
    }

    /**
     * input a integer.
     *
     * @param value value to encode
     * @param out the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(int value, ByteArrayOutputStream out) {
        values.add(value);
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // byteCache stores all <encoded-data> and we know its size
        ByteArrayOutputStream byteCache = new ByteArrayOutputStream();
        if (values.size() == 0) {
            return;
        }
        for (int value : values) {
            int n = int_to_zigzag(value);
            byte[] bytes = write_to_buffer(n);
            byteCache.write(bytes, 0, bytes.length);
        }
        byteCache.writeTo(out);
        reset();
    }

    private void reset() {
        values.clear();
    }
}
