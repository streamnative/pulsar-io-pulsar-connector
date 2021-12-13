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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarBytesSource implements Source<byte[]> {

    private Consumer<byte[]> consumer;

    private PulsarClient client;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        final PulsarSourceConfig sourceConfig = PulsarSourceConfig.load(config);
        final ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(sourceConfig.getServiceUrl());
        if (sourceConfig.isEnableTls()) {
            clientBuilder.allowTlsInsecureConnection(sourceConfig.isAllowTlsInsecureConnection())
                    .enableTlsHostnameVerification(sourceConfig.isEnableTlsHostnameVerification())
                    .tlsProtocols(sourceConfig.getTlsProtocols())
                    .tlsCiphers(sourceConfig.getTlsCiphers())
                    .useKeyStoreTls(sourceConfig.isUseKeyStoreTls())
                    .tlsTrustStorePath(sourceConfig.getTrustStorePath())
                    .tlsTrustStorePassword(sourceConfig.getTrustStorePassword())
                    .authentication(sourceConfig.getAuthenticationPluginClassName(), sourceConfig.getAuthenticationParams());
        }
        this.client = clientBuilder.build();
        this.consumer = this.client.newConsumer().topic(sourceConfig.getTopic())
                .subscriptionName(sourceConfig.getSubscriptionName())
                .receiverQueueSize(sourceConfig.getReceiverQueueSize())
                .autoUpdatePartitions(sourceConfig.isAutoUpdatePartition())
                .subscriptionInitialPosition(sourceConfig.getSubscriptionInitialPosition())
                .subscriptionType(sourceConfig.getSubscriptionType())
                .negativeAckRedeliveryDelay(sourceConfig.getNegativeAckRedeliveryDelay(), sourceConfig.getNegativeAckRedeliveryDelayTimeUnit())
                .subscribe();
    }

    @Override
    public Record<byte[]> read() throws Exception {
        final Message<byte[]> message = this.consumer.receive();
        return new PulsarRecord<>(message, () -> this.consumer.acknowledgeAsync(message), () -> this.consumer.negativeAcknowledge(message));
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
