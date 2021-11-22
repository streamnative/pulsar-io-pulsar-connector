package org.apache.pulsar.io.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * @author hezhangjian
 */
@Data
@Accessors(chain = true)
public class PulsarSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(required = true, defaultValue = "", help = "pulsar service url")
    private String serviceUrl;

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
    private int maxPendingMessages;

    public static PulsarSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), PulsarSinkConfig.class);
    }

    public static PulsarSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), PulsarSinkConfig.class);
    }

}
