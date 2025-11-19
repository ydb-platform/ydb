# Kafka API usage examples
<!-- markdownlint-disable blanks-around-fences -->

This example shows a code snippet for reading data from a topic via Kafka API without a consumer group (Manual Partition Assignment).
You don't need to create a consumer for this reading mode.

Before proceeding with the examples:

1. [Create a topic](../ydb-cli/topic-create.md).
1. [Add a consumer](../ydb-cli/topic-consumer-add.md).
1. If authentication is enabled, [create a user](../../security/authorization.md#user).

## How to try the Kafka API {#how-to-try-kafka-api}

### In Docker {#how-to-try-kafka-api-in-docker}

Run Docker following [the quickstart guide](../../quickstart.md#install), and the Kafka API will be available on port 9092.

## Kafka API usage examples

### Reading

{{ ydb-short-name }} Topics Kafka API lacks support for the [check.crcs](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs) option. Therefore, the following parameter must always be specified in the reader configuration: `check.crcs=false`.


Below are examples of reading using the Kafka protocol for various applications, programming languages, and frameworks without authentication.
For examples of how to set up authentication, see [Authentication examples](#authentication-examples).

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-read-no-auth.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-read-no-auth.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-read-no-auth.md) %}

- Spark

  {% include [index.md](_includes/java/kafka-api-spark-read-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/java/kafka-api-flink-read-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

{% endlist %}

### Writing

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-write-no-auth.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-write-no-auth.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-write-no-auth.md) %}

- Spark

  {% include [index.md](_includes/java/kafka-api-spark-write-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/java/kafka-api-flink-write-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

- Logstash

  {% include [index.md](_includes/logs-to-kafka/kafka-api-logstash.md) %}

- Fluent Bit

  {% include [index.md](_includes/logs-to-kafka/kafka-api-fluent-bit.md) %}

{% endlist %}

### Authentication examples {#authentication-examples}

For more details on authentication, see the [Authentication](./auth.md) section. Below are examples of authentication in a cloud database and a local database.

{% note info %}

Currently, the only available authentication mechanism with Kafka API in {{ ydb-short-name }} Topics is `SASL_PLAIN`.

{% endnote %}

#### Authentication examples in on-prem YDB

To use authentication in a multinode self-deployed database:

1. Create a user. [How to do this in YQL](../../yql/reference/syntax/create-user.md). [How to execute YQL from CLI](../ydb-cli/yql.md).
2. Connect to the Kafka API as shown in the examples below. In all examples, it is assumed that:

   - YDB is running locally with the environment variable `YDB_KAFKA_PROXY_PORT=9092`, meaning that the Kafka API is available at `localhost:9092`. For example, you can run YDB in Docker as described [here](../../quickstart.md#install).

   - <username> is the username you specified when creating the user.
   - <password> is the user's password you specified when creating the user.

Examples are shown for reading, but the same configuration parameters work for writing to a topic as well.

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-read-with-sasl-creds-on-prem.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-read-with-sasl-creds-on-prem.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-read-with-sasl-creds-on-prem.md) %}

{% endlist %}
