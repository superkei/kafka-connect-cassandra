# Example of how to use kafka connect

### Standalone

```
bin/connect-standalone connect-stndalone.properties cassandra-sink.properties 
```

run to compile a zip file in target folder

```
mvn clean install
```

unzip the file to plugin.path, i.e. /opt/plugins/kafka-connect-cassandra

run the command to startup

```
bin/connect-standalone connect-stndalone.properties cassandra-sink.properties
```

 

### Distributed (on kafka runtime env)

Copy connect-distributed.properties to config/, Run the following command to start the connect in distributed mode

```In distributed mode, creating/delete of connectors via REST api. The information will be stored in the kafka topic
bin/connect-distributed.sh config/connect-distributed.properties &
```

It will create topics in kafka to store the status, config and offset. REST API is needed to control create/delete/update the connectors.

For example, creating a new sink connector

```
PSOT http://kafka:8083/
Content-Type: application/json
Accept: application/json

{
    "name": "kafka-cassandra-sink-connector",
    "config": {
        "connector.class": "com.superkei.kafka.connect.cassandra.sink.CassandraSinkConnector",
        "tasks.max": "1",
        "topics": "example",
        "cassandra.host": "cassandra.host",
        "cassandra.port": "9042",
        "cassandra.keyspace.name": "cassandra_space",
        "max.retries": "10",
        "retry.backoff.ms": "3000",
        "batch.size": "100",
        "topics.valid.fields.example":"id,value,ts"       
    }
}
```

```
To view connectors
GET http://kafka:8083/connectors
```

```
To delete connector:
DELETE http://kafka:8083/connectors/kafka-cassandra-sink-connector
```



### Distributed (on kubernetes)

compile the project

```
cd kafka-connect-cassandra
mvn clean install
cd ..
```

build Docker image

```
docker build -t kafka-connect-sink:latest .
```

install into k8s via HELM

```
cd helm/kafka-connect-sink-chart
helm install kafka-connect-sink .
```

HELM will help you to install the connector into k8s. Your kafka connector lib jar file now embedded in the new image however it does not run automatically since the kafka connect is running in distributed mode. You will need to access the service on k8s, a simply way would be create a forwarding and access to the pod 8083 port and use above example (in section 2) to creating a new sink connector.