FROM confluentinc/cp-kafka-connect-base:5.5.0

COPY kafka-connect-cassandra/target/kafka-connect-cassandra-1.0.0-SNAPSHOT-package.zip /tmp/superkei-kafka-connect-cassandra-1.0.0.zip

RUN confluent-hub install --verbose --no-prompt /tmp/superkei-kafka-connect-cassandra-1.0.0.zip
