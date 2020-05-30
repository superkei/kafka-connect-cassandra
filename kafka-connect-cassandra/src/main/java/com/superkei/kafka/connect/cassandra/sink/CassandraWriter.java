/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.superkei.kafka.connect.cassandra.sink;

import com.superkei.kafka.connect.cassandra.utils.CachedSessionProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CassandraWriter {

    private static final Logger log = LoggerFactory.getLogger(CassandraWriter.class);

    private final CassandraSinkConnectorConfig config;
    final CachedSessionProvider cachedSessionProvider;

    CassandraWriter(final CassandraSinkConnectorConfig config) {
        this.config = config;
        this.cachedSessionProvider = new CachedSessionProvider(config.cassandraHost, config.cassandraUsername,
                config.cassandraPassword);
    }

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Session session = cachedSessionProvider.getValidConnection();
        final HashMap<String, PreparedStatement> preparedStatementMap = cachedSessionProvider.getPreparedStatementMap();

        log.debug("got session: {}", session);

        final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
        for (SinkRecord record : records) {
            final String tableName = getTableName(record.topic());
            BufferedRecords buffer = bufferByTable.get(tableName);
            if (buffer == null) {
                buffer = new BufferedRecords(config, config.cassandraKeyspace, tableName, session,
                        preparedStatementMap);
                bufferByTable.put(tableName, buffer);
            }
            log.debug("add record, key: {}, value: {} value type: {}", record.key(), record.value(),
                    record.value().getClass());
            buffer.add(record);
        }
        for (Map.Entry<String, BufferedRecords> entry : bufferByTable.entrySet()) {
            String tableName = entry.getKey();
            BufferedRecords buffer = entry.getValue();
            log.debug("Flushing records in Cassandra Writer for table name: {}", tableName);
            buffer.flush();
        }
    }

    private String getTableName(String topic) {
        return topic;
    }

    public void closeQuietly() {
        cachedSessionProvider.closeQuietly();
    }

}
