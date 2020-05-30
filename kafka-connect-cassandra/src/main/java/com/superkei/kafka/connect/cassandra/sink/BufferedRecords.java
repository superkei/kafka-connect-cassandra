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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final String keyspaceName;
    private final String tableName;
    private final CassandraSinkConnectorConfig config;
    private final Session session;
    private final HashMap<String, PreparedStatement> preparedStatementMap;

    private List<SinkRecord> records = new ArrayList<>();

    public BufferedRecords(CassandraSinkConnectorConfig config, String keyspaceName, String tableName, Session session,
            HashMap<String, PreparedStatement> preparedStatementMap) {
        this.config = config;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.session = session;
        this.preparedStatementMap = preparedStatementMap;
    }

    public void add(SinkRecord record) throws SQLException {
        log.debug("add record, topic: {}, key: {}, value: {}", record.topic(), record.key(), record.value());

        records.add(record);

        if (records.size() >= config.batchSize) {
            flush();
        }
    }

    public void flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return;
        }

        log.debug("Flushing {} buffered records", records.size());

        String validField = CassandraSinkTask.props.get("topics.valid.fields." + tableName);

        List<String> keys = null;

        if (validField == null || validField.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> valueMapTmp = (Map<String, ?>) records.get(0).value();

            if (valueMapTmp == null || valueMapTmp.isEmpty()) {
                log.debug("Records is empty");
                return;
            }
            // @SuppressWarnings("unchecked")
            keys = new ArrayList<String>(valueMapTmp.keySet());
        } else {
            keys = Arrays.asList(validField.trim().split(","));

        }

        if (!preparedStatementMap.containsKey(tableName)) {
            String cql = getInsertSql(tableName, keys);
            log.info("create prepare statement for {}: {}", tableName, cql);
            preparedStatementMap.put(tableName, session.prepare(cql));
        }

        PreparedStatement statement = preparedStatementMap.get(tableName);

        BatchStatement batchStmt = new BatchStatement();

        for (SinkRecord record : records) {

            @SuppressWarnings("rawtypes")
            Map valueMap = (Map) record.value();

            List<Object> objects = new ArrayList<Object>();

            boolean skipRecord = false;
            for (String key : keys) {
                Object obj = valueMap.get(key);
                if (obj == null) {
                    skipRecord = true;
                }
                if (obj instanceof Number) {
                    if (key.equals("ts") || key.contains("timestamp")) {
                        objects.add(new Date(((Number) obj).longValue()));
                    } else {
                        objects.add(obj);
                    }
                } else {
                    objects.add(obj);
                }
            }
            if (!skipRecord) {
                BoundStatement insert = new BoundStatement(statement).bind(objects.toArray());
                batchStmt.add(insert);
            } else {
                log.debug("Skipped record key: {} , value: {} ", record.key(), record.value());
            }
        }

        session.execute(batchStmt);
    }

    private String getInsertSql(String table, List<String> keys) {

        List<String> questions = new ArrayList<String>();

        for (int i = 0; i < keys.size(); i++) {
            questions.add("?");
        }

        List<String> columnsWithQuote = new ArrayList<String>(keys.size());

        keys.forEach(key -> {
            columnsWithQuote.add("\"" + key + "\"");
        });

        String columns = String.join(",", columnsWithQuote);

        String cql = "INSERT INTO " + keyspaceName + "." + table + "(" + columns + ")" + " VALUES("
                + String.join(",", questions) + ");";

        return cql;

    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }

}
