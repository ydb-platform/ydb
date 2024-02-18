
# Использование Kafka Connect

Инструмент [Kafka Connect](https://kafka.apache.org/documentation/#connect) предназначен для перемещения данных между Apache Kafka® и другими хранилищами данных.

Работа с данными в Kafka Connect осуществляется с помощью процессов-исполнителей (workers).

{% note warning %}

Экземпляры Kafka Connect для работы с YDB стоит разворачивать только в режиме одного процесса-исполнителя (standalone mode). YDB не поддерживает работу Kafka Connect в распределенном режиме (distributed mode).

{% endnote %}

Непосредственно перемещение данных выполняется с помощью коннекторов, которые запускаются в отдельных потоках процесса-исполнителя.

Подробную информацию о Kafka Connect и его настройке см. в документации [Apache Kafka®](https://kafka.apache.org/documentation/#connect).

### Настройка Kafka Connect
Пример настройки и запуска коннектора Kafka Connect для переноса данных из топика YDB в файл.

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
    file=/etc/kafka-connect-worker/file_to_write.json
    topics=Topic-1
    ```

    Где:

    * `file` — имя файла, в который коннектор будет писать данные.
    * `topics` — имя топика, из которого коннектор будет читать данные.

1. Запустите Kafka Connect в режиме Standalone:
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
file=/etc/kafka-connect-worker/file_to_read.json
topic=Topic-1
```

#### Из YDB в PostgreSQL
Пример файла настроек JDBCSink коннектора `/etc/kafka-connect-worker/jdbc-sink.properties` для переноса данных из топика в таблицу PostgreSQL. Используется коннектор [Kafka Connect JDBC Connector](https://github.com/confluentinc/kafka-connect-jdbc).
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
Пример файла настроек JDBCSource коннектора `/etc/kafka-connect-worker/jdbc-source.properties` для переноса данных из PostgreSQL таблицы в топик. Используется коннектор [Kafka Connect JDBC Connector](https://github.com/confluentinc/kafka-connect-jdbc).
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

#### Из YDB в S3
Пример файла настроек S3Sink коннектора `/etc/kafka-connect-worker/s3-sink.properties` для переноса данных из топика в S3. Используется коннектор [Aiven's S3 Sink Connector for Apache Kafka](https://github.com/Aiven-Open/s3-connector-for-apache-kafka).
```ini
name=s3-sink
connector.class=io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector
topics=Topic-1
aws.access.key.id=<s3-access-key>
aws.secret.access.key=<s3-secret>
aws.s3.bucket.name=<bucket-name>
aws.s3.endpoint=<s3-endpoint>
format.output.type=json
file.compression.type=none
```
