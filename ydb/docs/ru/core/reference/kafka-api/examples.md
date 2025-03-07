# Примеры чтения и записи по Kafka API
<!-- markdownlint-disable blanks-around-fences -->

В этой статье приведены примеры чтения и записи в [топики](../../concepts/topic.md) с использованием Kafka API.

Перед выполнением примеров:

1. [Создайте топик](../ydb-cli/topic-create.md).
1. [Добавьте читателя](../ydb-cli/topic-consumer-add.md).
1. Если у вас включена аутентификация, [создайте пользователя](../../yql/reference/syntax/create-user.md).

## Начало работы {#how-to-try-kafka-api}

### В Docker {#how-to-try-kafka-api-in-docker}

Запустите Docker по [этой](../../quickstart#install) инструкции. Kafka API будет доступен на 9092 порте.

## Примеры работы с Kafka API

### Чтение

При чтении отличительной особенностью Kafka API являются:

- отсутствие поддержки опции [check.crcs](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs);
- только одна стратегия назначения партиция - roundrobin;
- отсутствие возможности читать без предварительно созданной группы читателей.

Поэтому в конфигурации читателя всегда нужно указывать **имя группы читателей** и параметры:

- `check.crc=false`
- `partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor`

Ниже даны примеры чтения по Kafka протоколу для разных приложений, языков программирования и фреймворков подключения без аутентификации.
Примеры того, как настроить аутентификацию, смотри в разделе [Примеры с аутентификацией](#authentication-examples)

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-read-no-auth.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-read-no-auth.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-read-no-auth.md) %}

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  {% include [index.md](_includes/java/kafka-api-spark-read-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  {% include [index.md](_includes/java/kafka-api-flink-read-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

{% endlist %}

#### Частые проблемы и их решение

##### Ошибка Unexpected error in join group response

Полный текст ошибки:

```txt
Unexpected error in join group response: This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
```

Скорее всего проблема в том, что не указано имя читателя или указанное имя читателя не существует в кластере YDB.

Решение: создайте читателя с помощью [CLI](../ydb-cli/topic-consumer-add) или [SDK](../ydb-sdk/topic#alter-topic)

### Запись

{% note info %}

Сейчас не поддержана запись по Kafka API с использованием Kafka транзакций. Транзакции доступны только при использовании
[YDB Topic API](../ydb-sdk/topic.md#write-tx).

В остальном запись в Apache Kafka и в YDB Topics через Kafka API ничем не отличается.

{% endnote %}

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-write-no-auth.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-write-no-auth.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-write-no-auth.md) %}

- Spark

  {% include [index.md](_includes/spark-constraints.md) %}

  {% include [index.md](_includes/java/kafka-api-spark-write-no-auth.md) %}

  {% include [index.md](_includes/spark-version-notice.md) %}

- Flink

  {% include [index.md](_includes/flink-constraints.md) %}

  {% include [index.md](_includes/java/kafka-api-flink-write-no-auth.md) %}

  {% include [index.md](_includes/flink-version-notice.md) %}

- Logstash

  {% include [index.md](_includes/logs-to-kafka/kafka-api-logstash.md) %}

- Fluent Bit

  {% include [index.md](_includes/logs-to-kafka/kafka-api-fluent-bit.md) %}

{% endlist %}

### Примеры с аутентификацией {#authentication-examples}

Подробнее про аутентификацию смотри в разделе [Аутентификация](./auth.md). Ниже есть примеры аутентификации в облачной базе
и в локальной базе.

{% note info %}

Сейчас единственным доступным механизмом аутентификации с Kafka API в YDB Topics является `SASL_PLAIN`.

{% endnote %}

#### Примеры аутентификации в самостоятельно развернутом YDB

Для того, чтобы проверить работу с аутентификацией в локальной базе:

1. Создайте пользователя. [Как это сделать в YQL](../../yql/reference/syntax/create-user.md). [Как выполнить YQL из CLI](../ydb-cli/sql.md).
2. Подключитесь к Kafka API, как в примерах ниже. Во всех примерах предполагается, что:

  - YDB запущен локально с переменной окружения YDB_KAFKA_PROXY_PORT=9092 - то есть Kafka API доступен по адресу localhost:9092. Например можно поднять YDB в докере, как указано [здесь](../../quickstart.md#install).
  - <username> - это имя пользователя, которое вы указали при создании пользователя.
  - <password> - это пароль пользователя, который вы указали при создании пользователя.

Примеры показаны для чтения, но те же самые параметры конфигурации работают и для записи в топик.

{% list tabs %}

- Консольные утилиты Kafka

  {% include [index.md](_includes/kafka-console-utillities-java23-fix.md) %}

  {% include [index.md](_includes/bash/kafka-api-console-read-with-sasl-creds-on-prem.md) %}

- kcat

  {% include [index.md](_includes/bash/kafka-api-kcat-read-with-sasl-creds-on-prem.md) %}

- Java

  {% include [index.md](_includes/java/kafka-api-java-read-with-sasl-creds-on-prem.md) %}

{% endlist %}
