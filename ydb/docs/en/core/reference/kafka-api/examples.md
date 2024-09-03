# Kafka API usage examples

This article provides examples of Kafka API usage to work with [{{ ydb-short-name }} topics](../../concepts/topic.md).

Before executing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Examples of working with topics

The examples use:

* `ydb:9093` — host name and port.
* `/Root/Database` — database name.
* `/Root/Database/Topic-1` — topic name. It is allowed to specify either the full name (along with the database) or just the topic name.
* `user@/Root/Database` — username. The username includes the database name, which is specified after `@`.
* `*****` — user password.
* `consumer-1` — consumer name.


## Writing data to a topic

### Writing via Kafka Java SDK

This example includes a code snippet for writing data to a topic via [Kafka API](https://kafka.apache.org/documentation/).

  ```java
  String HOST = "ydb:9093";
  String TOPIC = "/Root/Database/Topic-1";
  String USER = "user@/Root/Database";
  String PASS = "*****";

  Properties props = new Properties();
  props.put("bootstrap.servers", HOST);
  props.put("acks", "all");

  props.put("key.serializer", StringSerializer.class.getName());
  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.serializer", StringSerializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("security.protocol", "SASL_SSL");
  props.put("sasl.mechanism", "PLAIN");
  props.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + USER + "\" password=\"" + PASS + "\";");

  props.put("compression.type", "none");

  Producer<String, String> producer = new KafkaProducer<>(props);
  producer.send(new ProducerRecord<String, String>(TOPIC, "msg-key", "msg-body"));
  producer.flush();
  producer.close();
  ```

### Writing via Logstash

To configure [Logstash](https://github.com/elastic/logstash), use the following parameters:

  ```
  output {
    kafka {
      codec => json
      topic_id => "/Root/Database/Topic-1"
      bootstrap_servers => "ydb:9093"
      compression_type => none
      security_protocol => SASL_SSL
      sasl_mechanism => PLAIN
      sasl_jaas_config => "org.apache.kafka.common.security.plain.PlainLoginModule required username='user@/Root/Database' password='*****';"
    }
  }
  ```

### Writing via Fluent Bit

To configure [Fluent Bit](https://github.com/fluent/fluent-bit), use the following parameters:

  ```
  [OUTPUT]
    name                          kafka
    match                         *
    Brokers                       ydb:9093
    Topics                        /Root/Database/Topic-1
    rdkafka.client.id             Fluent-bit
    rdkafka.request.required.acks 1
    rdkafka.log_level             7
    rdkafka.security.protocol     SASL_SSL
    rdkafka.sasl.mechanism        PLAIN
    rdkafka.sasl.username         user@/Root/Database
    rdkafka.sasl.password         *****
  ```

## Reading data from a topic

### Reading data from a topic via Kafka Java SDK

This example includes a code snippet for reading data from a topic via Kafka Java SDK.

```java
String HOST = "ydb:9093";
String TOPIC = "/Root/Database/Topic-1";
String USER = "user@/Root/Database";
String PASS = "*****";
String CONSUMER = "consumer-1";

Properties props = new Properties();

props.put("bootstrap.servers", HOST);

props.put("security.protocol", "SASL_SSL");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + USER + "\" password=\"" + PASS + "\";");

props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", StringDeserializer.class.getName());

props.put("check.crcs", false);
props.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());

props.put("group.id", CONSUMER);
Consumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList(new String[] {TOPIC}));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(10000); // timeout 10 sec
    for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.key() + ":" + record.value());
    }
}

```

### Reading data from a topic via Kafka Java SDK without a consumer group
This example shows a code snippet for reading data from a topic via Kafka API without a consumer group (Manual Partition Assignment).
You don't need to create a consumer for this reading mode.


```java
String HOST = "ydb:9093";
String TOPIC = "/Root/Database/Topic-1";
String USER = "user@/Root/Database";
String PASS = "*****";

Properties props = new Properties();

props.put("bootstrap.servers", HOST);

props.put("security.protocol", "SASL_SSL");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + USER + "\" password=\"" + PASS + "\";");

props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", StringDeserializer.class.getName());

props.put("check.crcs", false);
props.put("auto.offset.reset", "earliest"); // to read from start

Consumer<String, String> consumer = new KafkaConsumer<>(props);

List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
List<TopicPartition> topicPartitions = new ArrayList<>();

for (PartitionInfo partitionInfo : partitionInfos) {
    topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
}
consumer.assign(topicPartitions);

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(10000); // timeout 10 sec
    for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.key() + ":" + record.value());
    }
}

```

## Using Kafka Connect

The [Kafka Connect](https://kafka.apache.org/documentation/#connect) tool is designed to move data between Apache Kafka® and other data stores.

The data in Kafka Connect is handled by worker processes.

{% note warning %}

Kafka Connect instances for working with YDB should only be deployed in standalone mode. YDB does not support Kafka Connect in distributed mode.

{% endnote %}

The actual data movement is performed using connectors that run in separate threads of the executing process.

For more information about Kafka Connect and its configuration, see the [Apache Kafka®](https://kafka.apache.org/documentation/#connect) documentation.

### Setting up Kafka Connect

1. [Create a consumer](../ydb-cli/topic-consumer-add.md) with the name `connect-<connector-name>`. The connector name is specified in the configuration file when you set it up in the `name` field.

1. [Download](https://downloads.apache.org/kafka/) and unzip the Apache Kafka® archive:

    ```bash
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory /opt/kafka/
    ```

    This example uses Apache Kafka® version 3.6.1.


1. Create a directory with the executor process settings:


    ```bash
    sudo mkdir --parents /etc/kafka-connect-worker
    ```

1. Create the executor process settings file `/etc/kafka-connect-worker/worker.properties'

    ```ini
    # Main properties
    bootstrap.servers=ydb:9093

    # AdminAPI properties
    sasl.mechanism=PLAIN
    security.protocol=SASL_SSL
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<user>@<db>" password="<user-pass>";

    # Producer properties
    producer.sasl.mechanism=PLAIN
    producer.security.protocol=SASL_SSL
    producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<user>@<db>" password="<user-pass>";

    # Consumer properties
    consumer.sasl.mechanism=PLAIN
    consumer.security.protocol=SASL_SSL
    consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<user>@<db>" password="<user-pass>";

    consumer.partition.assignment.strategy=org.apache.kafka.kafka.clients.consumer.RoundRobinAssignor
    consumer.check.crcs=false

    # Converter properties
    key.converter=org.apache.kafka.connect.storage.StringConverter
    value.converter=org.apache.kafka.connect.storage.StringConverter
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false

    # Worker properties
    plugin.path=/etc/kafka-connect-worker/plugins
    offset.storage.file.filename=/etc/kafka-connect-worker/worker.offset
    ```

1. Create a FileSink connector settings file `/etc/kafka-connect-worker/file-sink.properties` to move data from YDB topics to a file:


    ```ini
    name=local-file-sink
    connector.class=FileStreamSink
    tasks.max=1
    file=/etc/kafka-connect-worker/file_to_write.json
    topics=Topic-1
    ```

    Where:

    * `file` - name of the file to which the connector will write data.
    * `topics` - the name of the topics from which the connector will read data.

1. Start Kafka Connect in Standalone mode:
    ```bash
    cd ~/opt/kafka/bin/ && \
    sudo ./connect-standalone.sh \
            /etc/kafka-connect-worker/worker.properties.
            /etc/kafka-connect-worker/file-sink.properties
    ```

### Sample settings files for other connectors

#### From File to YDB
Sample FileSource settings file of the connector `/etc/kafka-connect-worker/file-sink.properties` to move data from file to topic:
```ini
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=/etc/kafka-connect-worker/file_to_read.json
topic=Topic-1
```

#### From YDB to PostgreSQL
Sample JDBCSink connector `/etc/kafka-connect-worker/jdbc-sink.properties` configuration file for moving data from a topic to a PostgreSQL table. The [Kafka Connect JDBC Connector](https://github.com/confluentinc/kafka-connect-jdbc) is used.
```ini
name=postgresql-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector

connection.url=jdbc:postgresql://<postgresql-host>:<postgresql-port>/<db>
connection.user=<pg-user>
connection.password=<pg-user-pass>

topics=Topic-1
batch.size=2000
auto.commit.interval.ms=1000

transforms=wrap
transforms.wrap.type=org.apache.kafka.connect.transforms.HoistField$Value
transforms.wrap.field=data

auto.create=true
insert.mode=insert
pk.mode=none
auto.evolve=true
```

#### From PostgreSQL to YDB
Sample JDBCSource Connector `/etc/kafka-connect-worker/jdbc-source.properties` settings file for moving data from PostgreSQL table to topic. The [Kafka Connect JDBC Connector](https://github.com/confluentinc/kafka-connect-jdbc) is used.
```ini
name=postgresql-source
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector

connection.url=jdbc:postgresql://<postgresql-host>:<postgresql-port>/<db>
connection.user=<pg-user>
connection.password=<pg-user-pass>

mode=bulk
query=SELECT * FROM "Topic-1";
topic.prefix=Topic-1
poll.interval.ms=1000
validate.non.null=false
```

#### From YDB to S3
Sample S3Sink connector `/etc/kafka-connect-worker/s3-sink.properties` settings file for moving data from a topic to S3. The [Aiven's S3 Sink Connector for Apache Kafka](https://github.com/Aiven-Open/s3-connector-for-apache-kafka) is used.
```ini
name=s3-sink
connector.class=io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector
topics=Topic-1
aws.access.key.id=<s3-access-key>
aws.secret.access.key.key=<s3-secret>
aws.s3.bucket.name=<bucket-name>
aws.s3.endpoint=<s3-endpoint>
format.output.type=json
file.compression.type=none
```
