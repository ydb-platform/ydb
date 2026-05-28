# Примеры работы с топиком через SQS API
<!-- markdownlint-disable blanks-around-fences -->

В этой статье приведены примеры чтения и записи в [топики](../../concepts/datamodel/topic.md) с использованием SQS API.

{% include [x](_includes/limitations.md) %}

## Формирование endpoint для подключения

Endpoint для доступа к SQS API формируется следующим образом:

`https://{db-balancer}:{port}/{database}`

Где:

- `db-balancer` — DNS-имя HTTPS-балансировщика, у которого в качестве backend указаны compute-ноды базы данных (или адрес узла/сервиса, где запущен HTTP Proxy).
- `port` — порт, на котором доступен HTTP Proxy.
- `database` — полный путь базы данных, в которой находятся топики.

Этот endpoint указывается в AWS CLI через параметр `--endpoint`.

## Создание топика через AWS CLI

В SQS-протоколе операция создания вызывается как `CreateQueue`, но в {{ ydb-short-name }} она создаёт **топик** с указанным именем.

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue --queue-name "my_topic"
```

Для FIFO-топика используйте суффикс `.fifo` и атрибут `FifoQueue=true`:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue \
  --queue-name "my_topic.fifo" \
  --attributes FifoQueue=true
```

## Запись в топик и чтение из топика через AWS CLI

Для операций чтения и записи AWS CLI использует параметр `--queue-url`. Его можно получить через `get-queue-url`.

Ниже приведён пример:

- создание топика `my_topic`;
- получение `QueueUrl`;
- запись сообщения;
- чтение сообщения.

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

# создать топик (операция SQS CreateQueue)
aws --endpoint "$ENDPOINT" sqs create-queue --queue-name "my_topic"

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
