/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.MetricFactory;
import org.apache.iotdb.metrics.MetricReporter;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class MicrometerMetricReporter implements MetricReporter {
  MetricFactory micrometerMetricFactory;
  Thread runThread;

  @Override
  public boolean start() {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
      server.createContext(
          "/prometheus",
          httpExchange -> {
            String response =
                ((PrometheusMeterRegistry)
                        ((MicrometerMetricManager) micrometerMetricFactory.getMetric("iotdb"))
                            .getMeterRegistry())
                    .scrape();
            httpExchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = httpExchange.getResponseBody()) {
              os.write(response.getBytes());
            }
          });

      runThread = new Thread(server::start);
      runThread.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public void setMetricFactory(MetricFactory metricFactory) {
    micrometerMetricFactory = metricFactory;
  }

  @Override
  public boolean stop() {
    try {
      runThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }
}