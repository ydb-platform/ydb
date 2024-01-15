# Kafka API usage examples

This article provides examples of Kafka API usage to work with [{{ ydb-short-name }} topics](../../concepts/topic.md).


Before executing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Examples of working with topics

The examples use:

 * `ydb:9093` — host name.
 * `/Root/Database` — database name.
 * `/Root/Database/Topic` — topic name.
 * `user@/Root/Database` — username. Includes the username and database name.
 * `*****` — user password.


## Writing data to a topic

### Writing via Kafka Java SDK

This example includes a code snippet for writing data to a topic via [Kafka API](https://kafka.apache.org/documentation/).

  ```java
  String HOST = "ydb:9093";
  String TOPIC = "/Root/Database/Topic";
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
      topic_id => "/Root/Database/Topic"
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
    Topics                        /Root/Database/Topic
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
  String TOPIC = "/Root/Database/Topic";
  String USER = "user@/Root/Database";
  String PASS = "*****";

  Properties props = new Properties();
  props.put("bootstrap.servers", HOST);
  props.put("auto.offset.reset", "earliest"); // to read from start
  props.put("check.crcs", false);

  props.put("key.deserializer", StringDeserializer.class.getName());
  props.put("value.deserializer", StringDeserializer.class.getName());

  props.put("security.protocol", "SASL_SSL");
  props.put("sasl.mechanism", "PLAIN");
  props.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + USER + "\" password=\"" + PASS + "\";");

  Consumer<String, String> consumer = new KafkaConsumer<>(props);

  List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
  List<TopicPartition> topicPartitions = new ArrayList<>();

  for (PartitionInfo partitionInfo : partitionInfos) {
      topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
  }
  consumer.assign(topicPartitions);

  while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.key() + ":" + record.value());
      }
  }