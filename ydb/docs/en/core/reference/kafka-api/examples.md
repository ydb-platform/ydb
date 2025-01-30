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

Run Docker following [this guide](../../quickstart#install) and the Kafka API will be available on port 9092.

### In Yandex Cloud {#how-to-try-kafka-api-in-cloud}

You can try working with YDB topics via the Kafka API for free ([in small volumes](https://yandex.cloud/en/docs/data-streams/pricing?from=int-console-help-center-or-nav#prices)) in Yandex Cloud.
To do this in your [Yandex Cloud console](https://console.yandex.cloud):

1. Create a [YDB database](https://yandex.cloud/en/docs/ydb/quickstart) if you don't have one yet.
1. Create a [Yandex Data Streams queue](https://yandex.cloud/en/docs/data-streams/quickstart).
1. Create a [service account](https://yandex.cloud/en/docs/iam/operations/sa/create) if you don't have one yet,
   and assign this service account the roles of ydb.viewer (to read data from the stream), ydb.editor (to write data to the stream), and ydb.kafkaApi.client (to access the data stream via the Kafka API).
1. Create an [API key](https://yandex.cloud/en/docs/iam/operations/sa/create-access-key) for this service account.
   For the key, select `yc.ydb.topics.manage` in the **Scope** field. You can also set a description and an expiration date.


Authentication is required to work with Yandex Cloud, see authentication examples [below](#authentication-in-cloud-examples).

## Kafka API usage examples


### Reading

Consider the following points of using the Kafka API for reading:


- No support for the [check.crcs](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs) option.
- Only one partition assignment strategy - roundrobin.
- No reading without a pre-created consumer group.


Therefore, in the consumer configuration, you must always specify the **consumer group name** and the parameters:

- `check.crc=false`
- `partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor`

Below are examples of reading using the Kafka protocol for various applications, programming languages, and frameworks without authentication.
For examples of how to set up authentication, see the section [Authentication Examples](#authentication-examples)

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](../../../_includes/bash/kafka-api-console-read-no-auth.md) %}

- kcat

  {% include [index.md](../../../_includes/bash/kafka-api-kcat-read-no-auth.md) %}

- Java

  {% include [index.md](../../../_includes/java/kafka-api-java-read-no-auth.md) %}

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  {% include [index.md](../../../_includes/java/kafka-api-spark-read-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  {% include [index.md](../../../_includes/java/kafka-api-flink-read-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

{% endlist %}

#### Frequent problems and solutions

##### Unexpected error in join group response

Full text of an exception:

```txt
Unexpected error in join group response: This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
```

Most likely it means that a consumer group is not specified or, if specified, it does not exist in the YDB cluster.


Solution: create a consumer group in YDB using [CLI](../ydb-cli/topic-consumer-add) or [SDK](../ydb-sdk/topic#alter-topic).


### Writing

{% note info %}

Using Kafka transactions when writing via Kafka API is currently not supported. Transactions are only available when using the [YDB Topic API](https://ydb.tech/docs/ru/reference/ydb-sdk/topic#write-tx).

Otherwise, writing to Apache Kafka and YDB Topics through Kafka API is no different.

{% endnote %}

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](../../../_includes/bash/kafka-api-console-write-no-auth.md) %}

- kcat

  {% include [index.md](../../../_includes/bash/kafka-api-kcat-write-no-auth.md) %}

- Java

  {% include [index.md](../../../_includes/java/kafka-api-java-write-no-auth.md) %}

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  {% include [index.md](../../../_includes/java/kafka-api-spark-write-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  {% include [index.md](../../../_includes/java/kafka-api-flink-write-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

- Logstash

  {% include [index.md](../../../_includes/logs-to-kafka/kafka-api-logstash.md) %}

- Fluent Bit

  {% include [index.md](../../../_includes/logs-to-kafka/kafka-api-fluent-bit.md) %}

{% endlist %}

### Authentication examples {#authentication-examples}


For more details on authentication, see the section [Authentication](./auth.md). Below are examples of authentication in a cloud database and a local database.

{% note info %}

Currently, the only available authentication mechanism with Kafka API in YDB Topics is `SASL_PLAIN`.

{% endnote %}

#### Authentication examples in Yandex Cloud {#authentication-in-cloud-examples}


See instructions on how to start working with the Kafka API over YDB Topics in Yandex Cloud in the section [above](#how-to-try-kafka-api-in-cloud).


For authentication, add the following values to the Kafka connection parameters:

- `security.protocol` with value `SASL_SSL`
- `sasl.mechanism` with value `PLAIN`
- `sasl.jaas.config` with value `org.apache.kafka.common.security.plain.PlainLoginModule required username="@<path_to_database>" password="<Service Account API Key>";`

Below are examples of reading from a cloud topic, where:

- <path_to_database> is the path to the database from the topic page in YDS Yandex Cloud.
  ![path_to_database_example](./_assets/path_to_db_in_yds_cloud_ui.png)
- <kafka_api_endpoint> is the Kafka API Endpoint from the description page of YDS Yandex Cloud. It should be used as `bootstrap.servers`.
  ![kafka_endpoint_example](./_assets/kafka_api_endpoint_in_cloud_ui.png)
- <api_key> is the API Key of the service account that has access to YDS.


{% note info %}

The username is not specified in <path_to_database>. Only `@` is added, followed by the path to your database.

{% endnote %}

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](../../../_includes/bash/kafka-api-console-read-with-sasl-creds-cloud.md) %}

- kcat

  {% include [index.md](../../../_includes/bash/kafka-api-kcat-read-with-sasl-creds-cloud.md) %}

- Java

  {% include [index.md](../../../_includes/java/kafka-api-java-read-with-sasl-creds-cloud.md) %}

{% endlist %}

#### Authentication examples in on-prem YDB


To use authentication in a on-prem database:


1. Create a user. [How to do this in YQL](../../yql/reference/syntax/create-user.md). [How to execute YQL from CLI](../ydb-cli/yql.md).
2. Connect to the Kafka API as shown in the examples below. In all examples, it is assumed that:

   - YDB is running locally with the environment variable `YDB_KAFKA_PROXY_PORT=9092`, meaning that the Kafka API is available at `localhost:9092`. For example, you can run YDB in Docker as described [here](../../quickstart.md#install).

   - <username> is the username you specified when creating the user.
   - <password> is the user's password you specified when creating the user.

Examples are shown for reading, but the same configuration parameters work for writing to a topic as well.

{% list tabs %}

- Built-in Kafka CLI tools

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](../../../_includes/bash/kafka-api-console-read-with-sasl-creds-on-prem.md) %}

- kcat

  {% include [index.md](../../../_includes/bash/kafka-api-kcat-read-with-sasl-creds-on-prem.md) %}

- Java

  {% include [index.md](../../../_includes/java/kafka-api-java-read-with-sasl-creds-on-prem.md) %}

{% endlist %}