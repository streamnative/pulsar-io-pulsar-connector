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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarSinkTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = TestUtil.getFile("pulsar-sink-config.yaml");
        final PulsarSinkConfig config = PulsarSinkConfig.load(yamlFile.getAbsolutePath());
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getServiceUrl(), "pulsar://localhost:6650");
        Assert.assertTrue(config.isEnableTls());
        Assert.assertTrue(config.isAllowTlsInsecureConnection());
        Assert.assertTrue(config.isEnableTlsHostnameVerification());
        TestUtil.assertSetOnly(config.getTlsProtocols(), "TLSv1.2");
        TestUtil.assertSetOnly(config.getTlsCiphers(), "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        Assert.assertTrue(config.isUseKeyStoreTls());
        Assert.assertEquals(config.getTrustStorePath(), "/tmp/trustStore");
        Assert.assertEquals(config.getTrustStorePassword(), "fake-pwd");
        Assert.assertEquals(config.getAuthenticationPluginClassName(), "org.TestAuthPlugin");
        Assert.assertEquals(config.getAuthenticationParams(), new HashMap<String, String>() {
            {
                put("testKey", "testValue");
            }
        });
        Assert.assertEquals(config.getTopic(), "persistent://public/default/topic");
        Assert.assertTrue(config.isAutoUpdatePartition());
        Assert.assertEquals(config.getMaxPendingMessages(), 500);
    }

}
