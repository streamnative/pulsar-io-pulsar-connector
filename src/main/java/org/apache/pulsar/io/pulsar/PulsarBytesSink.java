/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.Map;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarBytesSink implements Sink<byte[]> {

    private Producer<byte[]> producer;

    private PulsarClient client;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        final PulsarSinkConfig sinkConfig = PulsarSinkConfig.load(config);
        final ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(sinkConfig.getServiceUrl());
        if (sinkConfig.isEnableTls()) {
            clientBuilder.allowTlsInsecureConnection(sinkConfig.isAllowTlsInsecureConnection())
                    .enableTlsHostnameVerification(sinkConfig.isEnableTlsHostnameVerification())
                    .tlsProtocols(sinkConfig.getTlsProtocols())
                    .tlsCiphers(sinkConfig.getTlsCiphers())
                    .useKeyStoreTls(sinkConfig.isUseKeyStoreTls())
                    .tlsTrustStorePath(sinkConfig.getTrustStorePath())
                    .tlsTrustStorePassword(sinkConfig.getTrustStorePassword())
                    .authentication(sinkConfig.getAuthenticationPluginClassName(), sinkConfig.getAuthenticationParams());
        }
        this.client = clientBuilder.build();
        this.producer = this.client.newProducer().topic(sinkConfig.getTopic()).maxPendingMessages(sinkConfig.getMaxPendingMessages())
                .autoUpdatePartitions(sinkConfig.isAutoUpdatePartition()).create();
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        final TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
        messageBuilder.value(record.getValue()).properties(record.getProperties());
        if (record.getKey().isPresent()) {
            messageBuilder.key(record.getKey().get());
        }
        messageBuilder.sendAsync().thenAccept(__ -> record.ack()).exceptionally(throwable -> {
            log.error("send msg to pulsar failed", throwable);
            return null;
        });
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
