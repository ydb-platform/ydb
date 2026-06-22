# Examples of working with a topic via SQS API
<!-- markdownlint-disable blanks-around-fences -->

This article provides examples of working with [topics](../../concepts/datamodel/topic.md) using the SQS API with [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html).

{% include [x](_includes/limitations.md) %}

{% include [x](_includes/examples_prerequisites.md) %}

## Building an endpoint for connection

The endpoint for accessing the SQS API is built as follows:

`https://{db-balancer}:{port}/{database}`

Where:

- `db-balancer` — DNS name of the HTTPS load balancer whose backends are the database compute nodes (or the address of the node/service where HTTP Proxy is running);
- `port` — port where HTTP Proxy is available;
- `database` — full database path where the topics are located.

This endpoint is specified in AWS CLI via the `--endpoint` parameter.

{% note info %}

The examples use the endpoint `https://my_db.balancer.example.com:8443/Root/my_db`. In it:

- `my_db.balancer.example.com` — DNS name of the load balancer where the SQS protocol is available;
- `8443` — network port;
- `/Root/my_db` — database name.

{% endnote %}

## Creating a topic

To create a topic, run:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue --queue-name "my_topic"
```

After running the command, a [topic](../../concepts/datamodel/topic.md) with the specified name and a [shared reader](../../concepts/datamodel/topic.md#shared-consumer) named `ydb-sqs-consumer` will be created. You can verify that the topic exists using the [scheme describe](../ydb-cli/commands/scheme-describe.md) command of [YDB CLI](../ydb-cli/index.md):

```shell
ydb -e grpcs://my_db.balancer.example.com:2135 -d /Root/my_db scheme describe my_topic
```

To create a FIFO queue, use the `FifoQueue=true` attribute. For FIFO queues, it is recommended to end the name with `.fifo` to follow the Amazon SQS naming convention:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" \
  sqs create-queue \
  --queue-name "my_topic.fifo" \
  --attributes FifoQueue=true
```

After running the command, a [topic](../../concepts/datamodel/topic.md) named `my_topic.fifo` and a [shared reader](../../concepts/datamodel/topic.md#shared-consumer) named `ydb-sqs-consumer` with message ordering enabled will be created.

## Getting a list of topics

To get a list of topics that can be accessed via the SQS protocol, run:

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

aws --endpoint "$ENDPOINT" sqs list-queues
```

## Writing to and reading from a topic

For read and write operations, AWS CLI uses the `--queue-url` parameter. You can obtain it via `get-queue-url`.

Below is an example of:

- obtaining `QueueUrl`;
- writing a message;
- reading a message.

```shell
ENDPOINT="https://my_db.balancer.example.com:8443/Root/my_db"

# get QueueUrl
QUEUE_URL="$(aws --endpoint "$ENDPOINT" sqs get-queue-url --queue-name "my_topic" --query 'QueueUrl' --output text)"

# write a message to the topic
aws --endpoint "$ENDPOINT" sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "hello from aws cli"

# read a message (long polling)
aws --endpoint "$ENDPOINT" sqs receive-message \
  --queue-url "$QUEUE_URL" \
  --wait-time-seconds 20 \
  --max-number-of-messages 1
```
