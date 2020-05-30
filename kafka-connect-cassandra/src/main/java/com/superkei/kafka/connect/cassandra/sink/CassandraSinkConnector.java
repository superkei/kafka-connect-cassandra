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

import com.superkei.kafka.connect.cassandra.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassandraSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(CassandraSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.debug("start...");
        configProps = props;
        if (log.isDebugEnabled()) {
            Set<Map.Entry<String, String>> entrySet = props.entrySet();
            for (Map.Entry<String, String> entry : entrySet) {
                log.debug("\t{} = {}", entry.getKey(), entry.getValue());
            }

        }
    }

    @Override
    public Class<? extends Task> taskClass() {

        return CassandraSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping Cassandra sink connector.");
    }

    @Override
    public ConfigDef config() {
        return CassandraSinkConnectorConfig.CONFIG_DEF;
    }

    public String getConfig(String key) {
        return configProps.get(key);
    }
}
