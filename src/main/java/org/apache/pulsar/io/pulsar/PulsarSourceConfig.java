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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Data
@Accessors(chain = true)
public class PulsarSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(required = true, defaultValue = "", help = "pulsar service url")
    private String serviceUrl;

    @FieldDoc(required = true, defaultValue = "", help = "pulsar subscribe name")
    private String subscriptionName;

    @FieldDoc(defaultValue = "false", help = "enable pulsar client tls")
    private boolean enableTls;

    @FieldDoc(defaultValue = "false", help = "allow tls insecure connection")
    private boolean allowTlsInsecureConnection;

    @FieldDoc(defaultValue = "true", help = "")
    private boolean enableTlsHostnameVerification;

    @FieldDoc(defaultValue = "", help = "tls protocols")
    private Set<String> tlsProtocols;

    @FieldDoc(defaultValue = "", help = "tls ciphers")
    private Set<String> tlsCiphers;

    @FieldDoc(defaultValue = "", help = "use key store tls")
    private boolean useKeyStoreTls;

    @FieldDoc(defaultValue = "", help = "trust store path")
    private String trustStorePath;

    @FieldDoc(defaultValue = "", sensitive = true, help = "trust store password")
    private String trustStorePassword;

    @FieldDoc(defaultValue = "", help = "tls authentication class name")
    private String authenticationPluginClassName;

    @FieldDoc(defaultValue = "", sensitive = true, help = "authentication params")
    private Map<String, String> authenticationParams;

    @FieldDoc(required = true, defaultValue = "", help = "topic")
    private String topic;

    @FieldDoc(required = true, defaultValue = "false", help = "")
    private boolean autoUpdatePartition;

    @FieldDoc(required = true, defaultValue = "1000", help = "max send queue size")
    private int receiverQueueSize;

    @FieldDoc(required = true, defaultValue = "", help = "subscription initial position")
    private SubscriptionInitialPosition subscriptionInitialPosition;

    @FieldDoc(required = true, defaultValue = "", help = "subscription type")
    private SubscriptionType subscriptionType;

    @FieldDoc(required = true, defaultValue = "1", help = "subscription type")
    private long negativeAckRedeliveryDelay;

    @FieldDoc(required = true, defaultValue = "MINUTES", help = "negative ack redelivery delay TimeUnit")
    private TimeUnit negativeAckRedeliveryDelayTimeUnit;

    public static PulsarSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), PulsarSourceConfig.class);
    }

    public static PulsarSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), PulsarSourceConfig.class);
    }

}
