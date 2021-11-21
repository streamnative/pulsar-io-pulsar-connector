package org.apache.pulsar.io.pulsar;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * @author hezhangjian
 */
@Data
@Accessors(chain = true)
public class PulsarSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

}
