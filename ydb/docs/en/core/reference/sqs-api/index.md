# SQS API

{{ ydb-short-name }} supports working with [topics](../../concepts/datamodel/topic.md) via the [SQS](https://en.wikipedia.org/wiki/Amazon_Simple_Queue_Service) protocol.

{% include [x](_includes/limitations.md) %}

A single topic can be used simultaneously via multiple protocols. For example, writes can be performed using the Topic API, and reads using the SQS API, and vice versa.

When creating a topic with the `CreateQueue` SQS API command, the topic is created with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning_modes) enabled: with one partition and the automatic ability to increase up to 10 active partitions. Auto-partitioning parameters can be changed via [YQL](../../yql/reference/syntax/alter-topic.md) or [YDB CLI](../ydb-cli/topic-alter.md).

## Reading via SQS API

For reading via SQS API, a [shared (common) reader](../../concepts/datamodel/topic.md#shared-consumer) is used, which must be created on the topic before reading via the SQS protocol begins. If the topic is created with the `CreateQueue` SQS API command, a shared (common) reader named `ydb-sqs-consumer` is automatically created.

Messages written via the Topic API can be compressed using [gzip](https://en.wikipedia.org/wiki/Gzip), [lzop](https://en.wikipedia.org/wiki/Lzop), or [zstd](https://en.wikipedia.org/wiki/Zstd). When reading via the SQS protocol, the server does not decompress them but passes them as base64: the reader must perform base64 decoding and then decompression.

Messages written via the Topic API without compression may contain binary data. When reading via the SQS protocol, the server also encodes them in base64 — the reader must perform base64 decoding.

To let the reader determine the compression algorithm, an attribute `BodyEncoding` is provided with each message. If the attribute `BodyEncoding` is missing, the message does not need to be decompressed. Possible attribute values: `gzip`, `lzop`, `zstd`, or `base64`.

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

When writing to a topic via the SQS protocol, messages are evenly distributed across partitions. This guarantees that all messages with the same `MessageGroupId` end up in the same partition.

For writing via the SQS protocol, message deduplication is supported by `DeduplicationMessageId`, and if `DeduplicationMessageId` is not passed, by message content. Deduplication by content can be enabled with the `CreateQueue` and `SetQueueAttributes` commands by specifying the `ContentBasedDeduplication` parameter.

Deduplication by content is implemented over a 5-minute window: a message with a duplicate `DeduplicationMessageId` can be written again after 5 minutes or more.

There is a limit on the number of messages that can be written to a topic partition with content-based deduplication enabled: 500 messages per second. If you need to write more messages to the topic, increase the number of partitions. The topic limit is calculated as 500 messages/sec/partition × number of partitions. For example, to write 10 thousand messages per second, create a topic with 20 partitions.

## Documentation sections

- [Authentication](auth.md)
- [Examples](examples.md)
- [Creating a queue using YQL](../../yql/reference/syntax/alter-topic.md#add-consumer)
