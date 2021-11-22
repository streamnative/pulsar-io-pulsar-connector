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
