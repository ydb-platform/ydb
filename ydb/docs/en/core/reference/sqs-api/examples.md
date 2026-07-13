# Examples of working with a topic via the Amazon SQS API

<!-- markdownlint-disable blanks-around-fences -->

This article provides examples of working with [topics](../../concepts/datamodel/topic.md) using the Amazon SQS API via [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html).

{% include [x](_includes/limitations.md) %}

{% include [x](_includes/examples_prerequisites.md) %}

## Forming an endpoint for connection

The endpoint for accessing the Amazon SQS API is formed as follows:

`https://{db-balancer}:{port}/{database}`

Where:

- `db-balancer` — the DNS name of the HTTPS load balancer whose backend includes the database compute nodes (or the address of the node/service where the HTTP Proxy is running).
- `port` — the port on which the HTTP Proxy is available.
- `database` — the full path of the database where the topics are located.

This endpoint is specified in AWS CLI via the `--endpoint` parameter.

{% note info %}

The examples use the endpoint `https://my_db.balancer.example.com:8443/Root/my_db`. It contains:

- `my_db.balancer.example.com` — the DNS name of the load balancer through which the Amazon SQS protocol is accessible.
- `8443` — the network port.
- `/Root/my_db` — the database name.

{% endnote %}

## Creating a topic

To create a topic, run the command:


```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue --queue-name "my_topic"
```


After running the command, a [topic](../../concepts/datamodel/topic.md) with the specified name and a [shared (common) reader](../../concepts/datamodel/topic.md#shared-consumer) named `ydb-sqs-consumer` will be created. You can verify the topic's existence using the [scheme describe](../ydb-cli/commands/scheme-describe.md) command of the [YDB CLI](../ydb-cli/index.md):


```shell
ydb -e grpcs://my_db.balancer.example.com:2135 -d /Root/my_db scheme describe my_topic
```


To create a FIFO queue, use the `FifoQueue=true` attribute. For FIFO queues, it is recommended to end the name with `.fifo` to comply with the Amazon SQS naming convention:


```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue \
  --queue-name "my_topic.fifo" \
  --attributes FifoQueue=true
```


After running the command, a [topic](../../concepts/datamodel/topic.md) named `my_topic.fifo` and a [shared (common) reader](../../concepts/datamodel/topic.md#shared-consumer) named `ydb-sqs-consumer` will be created, with message ordering preservation enabled.

## Getting the list of topics

To get the list of topics that can be accessed via the Amazon SQS protocol, run the command:


```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" sqs list-queues
```


### Getting the QueueUrl of a topic

To get the `QueueUrl` of a topic, run the command:


```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" sqs get-queue-url \
  --queue-name "my_topic@my_consumer"
```


Where:

- `{queue_name}` — the value of the `--queue-name` parameter, the Amazon SQS queue name in the format `{topic_name}@{consumer_name}`. If the [shared (common) reader](../../concepts/datamodel/topic.md#shared-consumer) is named `ydb-sqs-consumer`, it is sufficient to specify only `{topic_name}`.
- `{topic_name}` — the topic name with the path from the root of the database where it was created, for example `production/order` (topic `order` in directory `production`).
- `{consumer_name}` — the name of the [shared (common) reader](../../concepts/datamodel/topic.md#shared-consumer) on the topic, for example `ydb-sqs-consumer`.

In the example above, `my_topic` is `{topic_name}`, and `my_consumer` is `{consumer_name}`.

## Writing to and reading from a topic

For read and write operations, AWS CLI uses the `--queue-url` parameter. It can be obtained via `get-queue-url`.

Below is an example:

- getting `QueueUrl`.
- writing a message.
- reading a message.


```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

# get QueueUrl
QUEUE_URL="$(aws --endpoint "$ENDPOINT" sqs get-queue-url --queue-name "my_topic" --query 'QueueUrl' --output text)"

# write a message to a topic
aws --endpoint "$ENDPOINT" sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "hello from aws cli"

# read a message (long polling)
aws --endpoint "$ENDPOINT" sqs receive-message \
  --queue-url "$QUEUE_URL" \
  --wait-time-seconds 20 \
  --max-number-of-messages 1
```
