# Примеры использования Kafka API

В этой статье приведены примеры использования Kafka API для работы с [топиками](../../concepts/topic.md).

Перед выполнением примеров [создайте топик](../ydb-cli/topic-create.md) и [добавьте читателя](../ydb-cli/topic-consumer-add.md).

## Примеры работы с топиками

В примерах используются:

 * `ydb:9093` — имя хоста и порт.
 * `/Root/Database` — название базы данных.
 * `/Root/Database/Topic-1` — имя топика. Допускается указывать как полное имя (вместе с базой данных), так и только имя топика.
 * `user@/Root/Database` — имя пользователя. Имя пользователя включает название базы данных, которое указывается после `@`.
 * `*****` — пароль пользователя.
 * `consumer-1` — имя читателя.


## Запись данных в топик

### Запись через Kafka Java SDK

В этом примере приведен фрагмент кода для записи в топик через [Kafka API](https://kafka.apache.org/documentation/).

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

### Запись через Logstash

Для настройки [Logstash](https://github.com/elastic/logstash) используйте следующие параметры:

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

### Запись через Fluent Bit

Для настройки [Fluent Bit](https://github.com/fluent/fluent-bit) используйте следующие параметры:

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

## Чтение данных из топика

### Чтение данных из топика через Kafka Java SDK
В этом примере приведен фрагмент кода для чтения данных из топика через Kafka API.

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

### Чтение данных из топика через Kafka Java SDK без группы потребителей
В этом примере приведен фрагмент кода для чтения данных из топика через Kafka API без группы потребителей (Manual Partition Assignment).
При таком чтении можно не создавать читателя.


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

## Использование Kafka Connect

Инструмент [Kafka Connect](https://kafka.apache.org/documentation/#connect) предназначен для перемещения данных между Apache Kafka® и другими хранилищами данных.

Работа с данными в Kafka Connect осуществляется с помощью процессов-исполнителей (workers).

{% note warning %}

Экземпляры Kafka Connect для работы с YDB стоит разворачивать только в режиме одного процесса-исполнителя (standalone mode). YDB не поддерживает работу Kafka Connect в распределенном режиме (distributed mode).

{% endnote %}

Непосредственно перемещение данных выполняется с помощью коннекторов, которые запускаются в отдельных потоках процесса-исполнителя.

Подробную информацию о Kafka Connect и его настройке см. в документации [Apache Kafka®](https://kafka.apache.org/documentation/#connect).

### Настройка Kafka Connect

1. [Создайте читателя](../ydb-cli/topic-consumer-add.md) с именем `connect-<connector-name>`. Имя коннектора указывается в конфигурационном файле при его настройке в поле `name`.

1. [Скачайте](https://downloads.apache.org/kafka/) и распакуйте архив с Apache Kafka®:

    ```bash
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory /opt/kafka/
    ```

    В данном примере используется Apache Kafka® версии `3.6.1`.

1. Создайте каталог с настройками процесса-исполнителя:

    ```bash
    sudo mkdir --parents /etc/kafka-connect-worker
    ```

1. Создайте файл настроек процесса-исполнителя `/etc/kafka-connect-worker/worker.properties`

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

    consumer.partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor
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

1. Создайте файл настроек FileSink коннектора `/etc/kafka-connect-worker/file-sink.properties` для переноса данных из топика YDB в файл:

    ```ini
    name=local-file-sink
    connector.class=FileStreamSink
    tasks.max=1
    file=file_to_write.json
    topics=Topic-1
    ```

    Где:

    * `file` — имя файла, в который коннектор будет писать данные.
    * `topics` — имя топика, из которого коннектор будет читать данные.

1. Запустите Kafka Connect в режиме Standalone
    ```bash
    cd ~/opt/kafka/bin/ && \
    sudo ./connect-standalone.sh \
            /etc/kafka-connect-worker/worker.properties \
            /etc/kafka-connect-worker/file-sink.properties
    ```

### Примеры файлов настроек других коннекторов

#### Из файла в YDB
Пример файла настроек FileSource коннектора `/etc/kafka-connect-worker/file-sink.properties` для переноса данных из файла в топик:
```ini
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=file_to_read.json
topic=Topic-1
```

#### Из YDB в PostgreSQL
Пример файла настроек JDBCSink коннектора `/etc/kafka-connect-worker/jdbc-sink.properties` для переноса данных из топика в таблицу PostgreSQL:
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

#### Из PostgreSQL в YDB
Пример файла настроек JDBCSource коннектора `/etc/kafka-connect-worker/jdbc-source.properties` для переноса данных из PostgreSQL таблицы в топик:
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
