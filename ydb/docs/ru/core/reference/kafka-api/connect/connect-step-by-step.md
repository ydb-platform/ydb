# Настройка Kafka Connect. Пошаговая инструкция
В разделе приведена пошаговая инструкция по настройке коннектора Kafka Connect для копирования данных из топика {{ ydb-short-name }} в файл.

В инструкции используются:

* `<topic-name>` — имя топика. Допускается указывать как полное имя (вместе с путем базы данных), так и только имя топика.
* `<sasl.username>` — имя пользователя SASL. Подробности читайте в разделе [Аутентификация](../auth.md).
* `<sasl.password>` — пароль пользователя SASL. Подробности читайте в разделе [Аутентификация](../auth.md).

1. [Создайте читателя](../../ydb-cli/topic-consumer-add.md) с именем `connect-<connector-name>`. Имя коннектора указывается в конфигурационном файле при его настройке в поле `name`.

2. [Скачайте](https://downloads.apache.org/kafka/) и распакуйте архив с Apache Kafka®:

    ```bash
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory /opt/kafka/
    ```

    В данном примере используется Apache Kafka® версии `3.6.1`.

3. Создайте каталог с настройками процесса-исполнителя:

    ```bash
    sudo mkdir --parents /etc/kafka-connect-worker
    ```

4. Создайте файл настроек процесса-исполнителя `/etc/kafka-connect-worker/worker.properties`

    ```ini
    # Main properties
    bootstrap.servers=<ydb-endpoint>

    # AdminAPI properties
    sasl.mechanism=PLAIN
    security.protocol=SASL_SSL
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<sasl.username>" password="<sasl.password>";

    # Producer properties
    producer.sasl.mechanism=PLAIN
    producer.security.protocol=SASL_SSL
    producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<sasl.username>" password="<sasl.password>";

    # Consumer properties
    consumer.sasl.mechanism=PLAIN
    consumer.security.protocol=SASL_SSL
    consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<sasl.username>" password="<sasl.password>";

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

5. Создайте файл настроек FileSink коннектора `/etc/kafka-connect-worker/file-sink.properties` для переноса данных из топика YDB в файл:

    ```ini
    name=local-file-sink
    connector.class=FileStreamSink
    tasks.max=1
    file=/etc/kafka-connect-worker/file_to_write.json
    topics=<topic-name>
    ```

    Где:

    * `file` — имя файла, в который коннектор будет писать данные.
    * `topics` — имя топика, из которого коннектор будет читать данные.

6. Запустите Kafka Connect в режиме Standalone:
    ```bash
    cd ~/opt/kafka/bin/ && \
    sudo ./connect-standalone.sh \
            /etc/kafka-connect-worker/worker.properties \
            /etc/kafka-connect-worker/file-sink.properties
    ```