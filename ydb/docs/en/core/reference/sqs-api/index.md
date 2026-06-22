# SQS API

{{ ydb-short-name }} supports working with [topics](../../concepts/datamodel/topic.md) using the [SQS](https://en.wikipedia.org/wiki/Amazon_Simple_Queue_Service) protocol.

{% include [x](_includes/limitations.md) %}

A single topic can be accessed simultaneously via multiple protocols. For example, writes can be performed using the Topic API and reads using the SQS API, and vice versa.

When a topic is created with the SQS API `CreateQueue` command, it is created with [autopartitioning](../../concepts/datamodel/topic.md#autopartitioning_modes) enabled: one partition with automatic scaling up to 10 active partitions. You can change autopartitioning parameters via [YQL](../../yql/reference/syntax/alter-topic.md) or [YDB CLI](../ydb-cli/topic-alter.md).

## Reading via SQS API

Reading via SQS API uses a [shared reader](../../concepts/datamodel/topic.md#shared-consumer), which must be created on the topic before reading via the SQS protocol. If a topic is created with the SQS API `CreateQueue` command, a shared reader named `ydb-sqs-consumer` is created automatically.

Messages written via the Topic API may be compressed using [gzip](https://en.wikipedia.org/wiki/Gzip), [lzop](https://en.wikipedia.org/wiki/Lzop), or [zstd](https://en.wikipedia.org/wiki/Zstd). When reading via the SQS protocol, the server does not decompress them but passes them as base64: the reader must perform base64 decoding and then decompression.

Messages written via the Topic API without compression may contain binary data. When reading via the SQS protocol, the server also encodes them in base64 — the reader must perform base64 decoding.

To let the reader determine the compression algorithm, each message is delivered with a `BodyEncoding` attribute. If the `BodyEncoding` attribute is absent, decompression is not required. Possible attribute values: `gzip`, `lzop`, `zstd`, or `base64`.

Example of a message with compressed content:

```json
{
    "Messages": [
        {
            "MessageId": "D523647F-5E89-560B-B1D0-152201831603",
            "ReceiptHandle": "CAAQAA==",
            "MD5OfBody": "d620a162c499920254054f78eac4feed",
            "Body": "KLUv/QBYaQAAeWRiIHdyaXRlZCAyCg==",
            "Attributes": {
                "SentTimestamp": "1780660726888",
                "BodyEncoding": "zstd"
            }
        }
    ]
}
```

## Writing via SQS API

When writing to a topic via the SQS protocol, messages are distributed evenly across partitions. At the same time, it is guaranteed that all messages with the same `MessageGroupId` will end up in the same partition.

Writing via the SQS protocol supports message deduplication by `DeduplicationMessageId`, and if `DeduplicationMessageId` is not provided — by message content. You can enable content-based deduplication with the `CreateQueue` and `SetQueueAttributes` commands by specifying the `ContentBasedDeduplication` parameter.

Content-based deduplication is implemented with a 5-minute window: a message with a repeating `DeduplicationMessageId` can be written again after 5 minutes or more.

There is a limit on the number of messages that can be written per second to a topic partition with content-based deduplication enabled — 500 messages per second. If you need to write more messages to a topic, increase the number of partitions. The topic limit is calculated as 500 messages/sec/partition × number of partitions. For example, to write 10,000 messages per second, create a topic with 20 partitions.

## Documentation sections

- [Authentication](auth.md)
- [Examples](examples.md)
- [Creating a queue with YQL](../../yql/reference/syntax/alter-topic.md#add-consumer)
