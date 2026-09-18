# Examples of reading and writing via Kafka API

<!-- markdownlint-disable blanks-around-fences -->

This article provides examples of reading and writing to [topics](../../concepts/datamodel/topic.md) using Kafka API.

Before running the examples:

1. [Create a topic](../ydb-cli/topic-create.md).
2. [Add a reader](../ydb-cli/topic-consumer-add.md).
3. If authentication is enabled, [create a user](../../yql/reference/syntax/create-user.md).

## Getting started {#how-to-try-kafka-api}

### In Docker {#how-to-try-kafka-api-in-docker}

Run Docker according to [this](../../quickstart#install) instruction. Kafka API will be available on port 9092.

## Kafka API examples

### Reading

When reading, a distinctive feature of Kafka API is the lack of support for the [check.crcs](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs) option. Therefore, in the reader configuration, you always need to specify the parameter: `check.crcs=false`.

Below are examples of reading via the Kafka protocol for different applications, programming languages, and connection frameworks without authentication.
For examples of how to configure authentication, see the section [Examples with authentication](#authentication-examples).

{% list tabs %}

- Kafka console utilities

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

- Kafka console utilities

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

### Examples with authentication {#authentication-examples}

For more details on authentication, see the section [Authentication](./auth.md). Below are examples of authentication in a cloud database
and in a local database.

{% note info %}

Currently, the only available authentication mechanism with Kafka API in YDB Topics is `SASL_PLAIN`.

{% endnote %}

#### Examples of authentication in a self-hosted YDB

To test authentication in a local database:

1. Create a user. [How to do this in YQL](../../yql/reference/syntax/create-user.md). [How to run YQL from the CLI](../ydb-cli/sql.md).
2. Connect to Kafka API as in the examples below. All examples assume that:

- YDB is running locally with the environment variable YDB_KAFKA_PROXY_PORT=9092, meaning Kafka API is available at localhost:9092. For example, you can run YDB in Docker as described [here](../../quickstart.md#install).
- <username> is the username you specified when creating the user.
- <password> is the user password you specified when creating the user.

The examples are shown for reading, but the same configuration parameters also work for writing to a topic.

{% list tabs %}

- Kafka console utilities

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-read-with-sasl-creds-on-prem.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-read-with-sasl-creds-on-prem.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-read-with-sasl-creds-on-prem.md) %}

{% endlist %}
