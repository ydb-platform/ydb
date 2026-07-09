# Примеры работы с топиком через SQS API
<!-- markdownlint-disable blanks-around-fences -->

В этой статье приведены примеры работы с [топиками](../../concepts/datamodel/topic.md) с использованием SQS API с помощью [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html).

{% include [x](_includes/limitations.md) %}

{% include [x](_includes/examples_prerequisites.md) %}

## Формирование endpoint для подключения

Endpoint для доступа к SQS API формируется следующим образом:

`https://{db-balancer}:{port}/{database}`

Где:

- `db-balancer` — DNS-имя HTTPS-балансировщика, у которого в качестве backend указаны compute-ноды базы данных (или адрес узла/сервиса, где запущен HTTP Proxy);
- `port` — порт, на котором доступен HTTP Proxy;
- `database` — полный путь базы данных, в которой находятся топики.

Этот endpoint указывается в AWS CLI через параметр `--endpoint`.

{% note info %}

В примерах используется endpoint `https://my_db.balancer.example.com:8443/Root/my_db`. В нём:

- `my_db.balancer.example.com` — DNS-имя балансировщика, по которому доступен SQS-протокол;
- `8443` — сетевой порт;
- `/Root/my_db` — имя базы данных.

{% endnote %}

## Создание топика

Для создания топика выполните команду:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue --queue-name "my_topic"
```

После выполнения команды будет создан [топик](../../concepts/datamodel/topic.md) с указанным именем и [разделяемым (общим) читателем](../../concepts/datamodel/topic.md#shared-consumer) с именем `ydb-sqs-consumer`. Проверить существование топика можно с помощью команды [scheme describe](../ydb-cli/commands/scheme-describe.md) [YDB CLI](../ydb-cli/index.md):

```shell
ydb -e grpcs://my_db.balancer.example.com:2135 -d /Root/my_db scheme describe my_topic
```

Для создания FIFO-очереди используйте атрибут `FifoQueue=true`. Для FIFO-очередей рекомендуется заканчивать имя на `.fifo`, чтобы соответствовать соглашению об именовании Amazon SQS:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue \
  --queue-name "my_topic.fifo" \
  --attributes FifoQueue=true
```

После выполнения команды будет создан [топик](../../concepts/datamodel/topic.md) с именем `my_topic.fifo` и [разделяемым (общим) читателем](../../concepts/datamodel/topic.md#shared-consumer) с именем `ydb-sqs-consumer`, у которого включено сохранение порядка сообщений.

## Получение списка топиков

Для получения списка топиков, с которыми возможна работа по SQS протоколу, выполните команду:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" sqs list-queues
```

## Запись в топик и чтение из топика

Для операций чтения и записи AWS CLI использует параметр `--queue-url`. Его можно получить через `get-queue-url`.

Ниже приведён пример:

- получение `QueueUrl`;
- запись сообщения;
- чтение сообщения.

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

# получить QueueUrl
QUEUE_URL="$(aws --endpoint "$ENDPOINT" sqs get-queue-url --queue-name "my_topic" --query 'QueueUrl' --output text)"

# записать сообщение в топик
aws --endpoint "$ENDPOINT" sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "hello from aws cli"

# прочитать сообщение (long polling)
aws --endpoint "$ENDPOINT" sqs receive-message \
  --queue-url "$QUEUE_URL" \
  --wait-time-seconds 20 \
  --max-number-of-messages 1
```
