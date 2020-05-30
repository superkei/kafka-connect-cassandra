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
package com.superkei.kafka.connect.cassandra.utils;

import java.util.HashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedSessionProvider {
    private HashMap<String, PreparedStatement> preparedStatementMap = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(CachedSessionProvider.class);

    private final String host;
    private final String username;
    private final String password;
    private final Cluster cluster;
    private Session session;

    public CachedSessionProvider(String host) {
        this(host, null, null);
    }

    public CachedSessionProvider(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.cluster = Cluster.builder().addContactPoint(host).build();
    }

    public synchronized Session getValidConnection() {
        if (session == null) {
            newSession();
        } else if (session.isClosed()) {
            log.info("The Cassandra session is closed. Reconnecting...");
            closeQuietly();
            newSession();
        }
        return session;
    }

    private void newSession() {
        log.debug("Attempting to connect to {}", host);
        session = cluster.connect();
        preparedStatementMap.clear();
        ;
    }

    public synchronized void closeQuietly() {
        if (session != null) {
            try {
                session.close();
                session = null;
            } catch (Exception e) {
                log.warn("Ignoring error closing connection", e);
            }
        }
    }

    public HashMap<String, PreparedStatement> getPreparedStatementMap() {
        return preparedStatementMap;
    }

}
