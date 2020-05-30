/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.superkei.kafka.connect.cassandra.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class CassandraSinkConnectorConfig extends AbstractConfig {

    public final String cassandraHost;
    public final String cassandraKeyspace;
    public final String cassandraUsername;
    public final String cassandraPassword;
    public final int port;

    public final int retryBackoffMs;
    public final int maxRetries;
    public final int batchSize;

    public static final String CASSANDRA_KEYSPACE_NAME_CONFIG = "cassandra.keyspace.name";
    private static final String CASSANDRA_KEYSPACE_NAME_DOC = "Name of the Cassandra keyspace to write to.";
    public static final String CASSANDRA_HOST_CONFIG = "cassandra.host";
    private static final String CASSANDRA_HOST_DOC = "Host to connect to a Cassandra cluster.";

    public static final String PORT_CONFIG = "cassandra.port";
    private static final String PORT_DOC = "The port the Cassandra hosts are listening on.";
    public static final int PORT_CONFIG_DEFAULT = 9042;
    private static final String PORT_DOC_DISPLAY = "Port";
    private static final String PORT_GROUP = "Connection";

    public static final String USERNAME_CONFIG = "cassandra.username";
    public static final String USERNAME_CONFIG_DEFAULT = "cassandra";
    private static final String USERNAME_DOC = "The username to connect to Cassandra with.";
    private static final String USERNAME_DOC_DISPLAY = "Username";
    private static final String USERNAME_GROUP = "Connection";

    public static final String PASSWORD_CONFIG = "cassandra.password";
    private static final String PASSWORD_DOC = "The password to connect to Cassandra with.";
    public static final String PASSWORD_CONFIG_DEFAULT = "cassandra";
    private static final String PASSWORD_DOC_DISPLAY = "Password";
    private static final String PASSWORD_GROUP = "Connection";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";
    private static final String RETRIES_GROUP = "Retries";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 100;
    private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";
    private static final String WRITES_GROUP = "Writes";

    protected CassandraSinkConnectorConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);

        cassandraHost = getString(CASSANDRA_HOST_CONFIG);
        cassandraKeyspace = getString(CASSANDRA_KEYSPACE_NAME_CONFIG);
        cassandraUsername = getString(USERNAME_CONFIG);
        cassandraPassword = getString(PASSWORD_CONFIG);
        port = getInt(PORT_CONFIG);

        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        batchSize = getInt(BATCH_SIZE);

    }

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CASSANDRA_KEYSPACE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    CASSANDRA_KEYSPACE_NAME_DOC)
            .define(CASSANDRA_HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CASSANDRA_HOST_DOC)
            .define(PORT_CONFIG, ConfigDef.Type.INT, PORT_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, PORT_DOC,
                    PORT_GROUP, 1, ConfigDef.Width.SHORT, PORT_DOC_DISPLAY)
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, USERNAME_CONFIG_DEFAULT, ConfigDef.Importance.HIGH,
                    USERNAME_DOC, USERNAME_GROUP, 3, ConfigDef.Width.SHORT, USERNAME_DOC_DISPLAY)
            .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, PASSWORD_CONFIG_DEFAULT, ConfigDef.Importance.HIGH,
                    PASSWORD_DOC, PASSWORD_GROUP, 4, ConfigDef.Width.SHORT, PASSWORD_DOC_DISPLAY)
            .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC,
                    WRITES_GROUP, 2, ConfigDef.Width.SHORT, BATCH_SIZE_DISPLAY)
            .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC, RETRIES_GROUP, 2, ConfigDef.Width.SHORT, RETRY_BACKOFF_MS_DISPLAY)
            .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC,
                    RETRIES_GROUP, 1, ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY);

}
