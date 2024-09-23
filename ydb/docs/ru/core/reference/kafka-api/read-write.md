# Примеры чтения и записи по Kafka API

В этой статье приведены примеры чтения и записи в [топики](../../concepts/topic.md) с использованием Kafka API.

Перед выполнением примеров:
1. [Создайте топик](../ydb-cli/topic-create.md).
1. [Добавьте читателя](../ydb-cli/topic-consumer-add.md).
1. [Создайте пользователя](../../security/access-management.md#users).

В примерах используются:

 * `<ydb-endpoint>` — эндпоинт {{ ydb-short-name }}.
 * `<topic-name>` — имя топика. Допускается указывать как полное имя (вместе с путем базы данных), так и только имя топика.
 * `<sasl.username>` — имя пользователя SASL. Подробности читайте в разделе [Аутентификация](./auth.md).
 * `<sasl.password>` — пароль пользователя SASL. Подробности читайте в разделе [Аутентификация](./auth.md).
 * `<consumer-name>` — [имя читателя](../../concepts/topic#consumer).


## Запись данных в топик
{% list tabs %}
- kcat
  ```ini
  echo "test message" | kcat -P \
    -b <ydb-endpoint> \
    -t <topic-name> \
    -k key \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=PLAIN \
    -X sasl.username="<sasl.username>" \
    -X sasl.password="<sasl.password>" \
  ```

- Java
  ```java
  String HOST = "<ydb-endpoint>";
  String TOPIC = "<topic-name>";
  String USER = "<sasl.username>";
  String PASS = "<sasl.password>";

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

- Logstash
  ```
  output {
    kafka {
      codec => json
      topic_id => "<topic-name>"
      bootstrap_servers => "<ydb-endpoint>"
      compression_type => none
      security_protocol => SASL_SSL
      sasl_mechanism => PLAIN
      sasl_jaas_config => "org.apache.kafka.common.security.plain.PlainLoginModule required username='<sasl.username>' password='<sasl.password>';"
    }
  }
  ```

- Fluent Bit

  ```
  [OUTPUT]
    name                          kafka
    match                         *
    Brokers                       <ydb-endpoint>
    Topics                        <topic-name>
    rdkafka.client.id             Fluent-bit
    rdkafka.request.required.acks 1
    rdkafka.log_level             7
    rdkafka.security.protocol     SASL_SSL
    rdkafka.sasl.mechanism        PLAIN
    rdkafka.sasl.username         <sasl.username>
    rdkafka.sasl.password         <sasl.password>
  ```
{% endlist %}
## Чтение данных из топика
{% list tabs %}

- kcat
  ```ini
  kcat -C \
      -b <ydb-endpoint> \
      -X security.protocol=SASL_SSL \
      -X sasl.mechanism=PLAIN \
      -X sasl.username="<sasl.username>" \
      -X sasl.password="<sasl.password>" \
      -X partition.assignment.strategy=roundrobin \
      -G <consumer-name> <topic-name>
  ```

- Java
  ```java
  String HOST = "<ydb-endpoint>";
  String TOPIC = "<topic-name>";
  String USER = "<sasl.username>";
  String PASS = "<sasl.password>";
  String CONSUMER = "<consumer-name>";

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
{% endlist %}
