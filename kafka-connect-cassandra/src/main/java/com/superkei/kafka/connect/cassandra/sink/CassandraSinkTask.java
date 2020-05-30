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

import com.superkei.kafka.connect.cassandra.utils.Version;
import com.datastax.driver.core.Cluster;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CassandraSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);

    private CassandraSinkConnectorConfig config;
    private CassandraWriter writer;
    private Cluster cluster;
    private int remainingRetries;

    public static Map<String, String> props = new HashMap<String, String>();

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting");
        if (log.isDebugEnabled()) {
            Set<Map.Entry<String, String>> entrySet = props.entrySet();
            for (Map.Entry<String, String> entry : entrySet) {
                log.debug("\t{} = {}", entry.getKey(), entry.getValue());
            }
        }
        CassandraSinkTask.props.putAll(props);
        this.config = new CassandraSinkConnectorConfig(CassandraSinkConnectorConfig.CONFIG_DEF, props);
        remainingRetries = config.maxRetries;
        initWriter();
    }

    void initWriter() {
        log.info("Creating Cassandra client.");
        this.cluster = Cluster.builder().addContactPoint(this.config.cassandraHost).build();
        log.info("Created Cassandra client {}.", this.cluster);
        log.info("Initializing writer");
        writer = new CassandraWriter(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            log.debug("No records read from Kafka.");
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug("Processing " + recordsCount + " records from Kafka:");
        log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to Cassandra...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
        try {
            writer.write(records);
        } catch (Exception e) {
            log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, e);
            if (remainingRetries == 0) {
                throw new ConnectException(e);
            } else {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            }
        }
        remainingRetries = config.maxRetries;
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        writer.closeQuietly();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

}