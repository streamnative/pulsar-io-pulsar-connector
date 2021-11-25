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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;

import java.util.Map;
import java.util.Optional;

/**
 * @author hezhangjian
 */
public class PulsarRecord<T> implements Record<T> {

    private final Message<T> message;

    private final Runnable ackFunction;

    private final Runnable failFunction;

    public PulsarRecord(Message<T> message, Runnable ackFunction, Runnable failFunction) {
        this.message = message;
        this.ackFunction = ackFunction;
        this.failFunction = failFunction;
    }

    @Override
    public Optional<String> getKey() {
        if (message.hasKey()) {
            return Optional.of(message.getKey());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return message.getProperties();
    }

    @Override
    public T getValue() {
        return message.getValue();
    }

    @Override
    public void ack() {
        this.ackFunction.run();
    }

    @Override
    public void fail() {
        this.failFunction.run();
    }
}
