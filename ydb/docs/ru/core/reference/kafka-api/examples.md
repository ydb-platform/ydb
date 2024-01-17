# Примеры использования Kafka API

В этой статье приведены примеры использования Kafka API для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

## Примеры работы с топиками

В примерах используются:

 * `ydb:9093` — имя хоста.
 * `/Root/Database` — название базы данных.
 * `/Root/Database/Topic` — имя топика.
 * `user@/Root/Database` — имя пользователя. Имя пользователя указывается полностью и включает название базы данных.
 * `*****` — пароль пользователя.


## Запись данных в топик

### Запись через Kafka Java SDK

В этом примере приведен фрагмент кода для записи в топик через [Kafka API](https://kafka.apache.org/documentation/).

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

### Запись через Logstash

Для настройки [Logstash](https://github.com/elastic/logstash) используйте следующие параметры:

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

### Запись через Fluent Bit

Для настройки [Fluent Bit](https://github.com/fluent/fluent-bit) используйте следующие параметры:

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

## Чтение данных из топика

### Чтение данных из топика через Kafka Java SDK

В этом примере приведен фрагмент кода для чтения данных из топика через Kafka API.

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