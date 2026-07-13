# Shared topic reader: architecture and limitations

A [shared (common) reader](../concepts/datamodel/topic.md#shared-consumer) is a reading model for a [topic](../concepts/datamodel/topic.md) where one or more messages, rather than an entire partition, are assigned to a consumer. This allows multiple consumers to process messages from a single partition in parallel and use topics as message queues. A typical scenario is message exchange between microservices; reading via the [Amazon SQS API](../reference/sqs-api/index.md) works through a shared reader.

For a general description of the model, reader settings, DLQ policies, and message ordering, see the [Shared (common) reader](../concepts/datamodel/topic.md#shared-consumer) section. This article describes the internal architecture of the mechanism: how the server tracks processing state, distributes read requests across partitions, what limitations to consider when designing high-load queues, and what resource consumption the inflight creates.

## Implementation specifics {#shared-consumer-implementation}

For each pair of 'shared (common) reader — topic partition', the server maintains its own message processing state. The state consists of a continuous block of messages from the topic partition — the inflight. The first message of the inflight is a message that has not yet been processed, is being processed, or is waiting to be moved to the DLQ. If the first message is successfully processed, the beginning of the block moves to the next message. Messages for processing are only delivered from the messages that are in the inflight.

Up to 120,000 messages can be in the inflight of a single partition. This means that if a large number of consumers (more than 120,000) read from one partition, no more than 120,000 consumers will receive messages for processing, even if there are more messages in the partition. Increasing the number of partitions in a topic increases the total inflight size — the total inflight of a topic equals the number of partitions multiplied by 120,000 messages. If you need to process a large number of messages simultaneously, you should increase the number of topic partitions.

If a topic consists of several partitions, read requests are evenly distributed across all partitions. A situation may occur where a request hits a partition that has no messages. In this case, the consumer will receive a response that there are no messages to process. If a partition has no messages to process for a long time, it is excluded from the distribution of read requests. The partition will return to the distribution as soon as messages to process appear in it.

For shared (common) readers with message ordering preservation, this means that if a large number of messages from one message-group-id (more than 120,000 messages) are written consecutively to a partition, the entire inflight will be occupied by messages from one message-group-id, and messages from other groups will not be delivered until messages from other groups appear in the inflight.

## Fair queues {#fair-queues}

When delivering messages for reading, the server takes into account the `message-group-id` of messages in the inflight. By trying to deliver messages from different groups, the server distributes the load among writers more evenly.

This allows smoothing out peaks from individual writers: if one writer sends many more messages to the queue than others, processing of messages from 'small' writers gets priority and is not blocked by the flow from a single source.

For [FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html) and shared readers with message ordering preservation, the order of messages within a single `message-group-id` is guaranteed: the consumer always receives the group's messages in the order they were written.

## Resource consumption {#resource-usage}

The inflight state consumes RAM and disk resources. For each message in the inflight, the server stores about 32 bytes of metadata — this is not the message body, but metadata about its processing status.

### RAM

The entire inflight is stored in the server's memory. To estimate memory consumption, it is convenient to calculate per million messages: **1 million messages in the inflight ≈ 32 MB** of RAM.

For example, a topic with 10 partitions and one shared consumer with a fully filled inflight (120,000 messages per partition) holds up to 1.2 million messages in inflight — about **38 MB** of memory for the service state alone. For one pair of "shared consumer — partition" at maximum inflight, it amounts to about **3.8 MB**. If a topic has multiple shared consumers, memory consumption multiplies by their number.

When designing, consider:

- The more partitions and consumers, the higher the memory consumption ceiling.
- The longer messages are being processed (long message processing time, slow consumers), the longer they stay in inflight and occupy memory.
- Increasing the number of partitions expands parallelism, but at the same time increases the total inflight volume.

### Disk

Information about message status is also stored on disk — from **32 bytes per message**. Storage consists of two parts:

1. **Snapshot** — the full state of all messages in inflight is periodically written.
2. **Change log** — the server processes pending read requests in batches and, when processing each batch, appends message status changes to the log.

Therefore, high read frequency and large inflight increase the volume of disk writes. When planning cluster capacity, allocate disk resources alongside RAM — especially for topics with a large number of partitions, multiple shared consumers, and long message processing times.
