# Configuring Kafka Connect. A step-by-step guide

This section provides a step-by-step guide for configuring a Kafka Connect connector to copy data from a {{ ydb-short-name }} topic to a file.

The following placeholders are used in this guide:

* `<topic-name>` — the topic name. You can specify either the full name (including the database path) or just the topic name.
* `<sasl.username>` — the SASL username. For more details, see the [Authentication](../auth.md) section.
* `<sasl.password>` — the SASL password. For more details, see the [Authentication](../auth.md) section.

1. [Create a consumer](../../ydb-cli/topic-consumer-add.md) named `connect-<connector-name>`. The connector name is specified in the `name` field of its configuration file.

2. [Download](https://downloads.apache.org/kafka/) and unpack the Apache Kafka® archive:

    ```bash
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory /opt/kafka/
    ```

    This example uses Apache Kafka® version `3.6.1`.

3. Create a directory for the worker process configuration:

    ```bash
    sudo mkdir --parents /etc/kafka-connect-worker
    ```

4. Create the worker process configuration file `/etc/kafka-connect-worker/worker.properties`:

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

    consumer.check.crcs=false

    # Converter properties
    key.converter=org.apache.kafka.connect.storage.StringConverter
    value.converter=org.apache.kafka.connect.storage.StringConverter
    key.converter.schemas.enable=false
    value.converter.schemas.enable=false

    # Worker properties
    plugin.path=/etc/kafka-connect-worker/plugins
    ```

5. Create the configuration file `/etc/kafka-connect-worker/file-sink.properties` for the FileSink connector to copy data from a {{ ydb-short-name }} topic to a file:

    ```ini
    name=local-file-sink
    connector.class=FileStreamSink
    tasks.max=1
    file=/etc/kafka-connect-worker/file_to_write.json
    topics=<topic-name>
    ```

    Where:

    * `file` - the name of the file where the connector will write data.
    * `topics` - the name of the topic from which the connector will read data.

6. Run Kafka Connect in standalone mode:

    ```bash
    cd ~/opt/kafka/bin/ && \
    sudo ./connect-standalone.sh \
            /etc/kafka-connect-worker/worker.properties \
            /etc/kafka-connect-worker/file-sink.properties
    ```