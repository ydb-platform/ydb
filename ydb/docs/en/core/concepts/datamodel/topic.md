# Topic

A topic in {{ ydb-short-name }} is an entity for storing unstructured messages and delivering them to multiple subscribers. Basically, a topic is a named set of messages.

<<<<<<< HEAD
A producer app writes messages to a topic. Consumer apps are independent of each other, they receive and read messages from the topic in the order they were written there. Topics implement the [publish-subscribe](https://en.wikipedia.org/wiki/Publish–subscribe_pattern) architectural pattern.
=======
A writer application writes messages to a topic. Reader applications are independent of each other; they receive, or "read", messages from the topic in the order they were written. The topic implements the [publisher-subscriber](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) architectural pattern.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

{{ ydb-short-name }} topics have the following properties:

* At-least-once delivery guarantees when messages are read by subscribers.
* Exactly-once delivery guarantees when publishing messages (to ensure there are no duplicate messages).
* [FIFO](https://en.wikipedia.org/wiki/Message_queue) message processing guarantees for messages published with the same [source ID](#producer-id).
* Message delivery bandwidth scaling for messages published with different sequence IDs.

## Messages {#message}

Data is transferred as message streams. A message is the minimum atomic unit of user information. A message consists of a body, attributes, and additional system properties. The content of a message is an array of bytes which is not interpreted by {{ydb-short-name}} in any way.

Messages may contain user-defined attributes in "key-value" format. They are returned along with the message body when reading the message. User-defined attributes let the consumer decide whether it should process the message without unpacking the message body. Message attributes are set when initializing a write session. This means that all messages written within a single write session will have the same attributes when reading them.

## Partitioning {#partitioning}

To enable horizontal scaling, a topic is divided into `partitions` that are units of parallelism. Each partition has a limited throughput. The recommended write speed is 1 MBps.

{% note info %}

As of now, you can only reduce the number of partitions in a topic by deleting and recreating a topic with a smaller number of partitions.

{% endnote %}

Partitions can be:

<<<<<<< HEAD
 - **Active.** By default, all partitions are active. Both read and write operations are allowed on an active partition.
 - **Inactive.** An inactive partition is read-only. A partition becomes inactive after splitting for [autopartitioning](#autopartitioning). It is automatically deleted once all messages are removed due to the expiration of the retention period.

### Offset {#offset}

All messages within a partition have a unique sequence number called an `offset`. An offset monotonically increases as new messages are written.
=======
- **Active.** All partitions by default; both writing and reading are supported.
- **Inactive.** Only reading is possible from them. Inactive partitions appear after a partition split when [auto-partitioning](#autopartitioning) is enabled. Inactive partitions are automatically deleted when all messages from such a partition have been deleted after the retention period expires.

### Offset {#offset}

All messages within a partition have a unique sequence number called `offset` (offset). The offset increases monotonically as new messages are written.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

## Autopartitioning {#autopartitioning}

<<<<<<< HEAD
Total topic throughput is determined by the number of partitions in the topic and the throughput of each partition. The number of partitions and the throughput of each partition are set at the time of topic creation. If the maximum required write speed for a topic is unknown at the creation time, autopartitioning allows the topic to be scaled automatically. If autopartitioning is enabled for a topic, the number of partitions will increase automatically as the write speed increases (see [Autopartitioning Strategies](#autopartitioning_strategies)).

### Guarantees {#autopartitioning_guarantee}

1. The SDK and server provide an exactly-once guarantee in the case of writing during a partition split. This means that any message will be written either to the parent partition or to one of the child partitions but never to both simultaneously. Additionally, a message cannot be written to the same partition multiple times.
2. The SDK and server maintain the reading order. Data is read from the parent partition first, followed by the child partitions.
3. As a result, the exactly-once writing guarantee and the reading order guarantee are preserved for a specific [producer identifier](#producer-id).
=======
The number of topic partitions and their throughput are set when creating the topic and determine the total write throughput of the topic. If the maximum required write speed to the topic is unknown at creation time or will change over time, you can use auto-partitioning to dynamically scale the topic. If auto-partitioning up is enabled on the topic, the number of partitions in such a topic automatically increases as the write speed grows (see [Auto-partitioning modes](#autopartitioning_modes) for details).

### Guarantees {#autopartitioning_guarantee}

1. The SDK and server provide exactly-once write guarantees in case of partition splits. This means that any message will be written either to the parent partition or to one of the child partitions. A message cannot be written to both the parent and child partitions simultaneously. Moreover, a message cannot be written to the same partition multiple times.
2. The SDK and server ensure read order. Data will be read from parent partitions first, and only then from child partitions.
3. Thus, the exactly-once write and read order guarantees continue to hold for a specific [source identifier (producer-id)](#producer-id).
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Autopartitioning Strategies {#autopartitioning_strategies}

The following autopartitioning strategies are available for any topic:

#### DISABLED

Autopartitioning is disabled for this topic. The number of partitions remains constant, and there is no automatic scaling.

The initial number of partitions is set during topic creation. If the partition count is manually adjusted, new partitions are added. Both previously existing and new partitions are active.

#### UP

<<<<<<< HEAD
Upwards autopartitioning is enabled for this topic. This means that if the write speed to the topic increases, the number of partitions will automatically increase. However, if the write speed decreases, the number of partitions remains unchanged.

The partition count increase algorithm works as follows: if the write speed for a partition exceeds a defined threshold (as a percentage of the maximum write speed for that partition) during a specified period, the partition is split into two child partitions. The original partition becomes inactive, allowing only read operations. When the retention period expires, and all messages in the original partition are deleted, the partition itself is also deleted. The two new child partitions become active, allowing both read and write operations.
=======
The topic has auto-partitioning 'up' enabled, meaning that when the write speed increases, the number of partitions increases. When the write speed decreases, the number of partitions remains unchanged.

Algorithm for increasing the number of partitions: if within a specified time the write speed to a partition exceeds the specified threshold (as a percentage of the maximum write speed to the partition), that partition is split into two. The original partition becomes inactive, and data can only be read from it. When the message retention period for that partition expires and all messages are deleted, the partition itself is also deleted. The two new child partitions become active, and both reading and writing are possible in them.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

#### PAUSED

Autopartitioning is paused for this topic, meaning that the number of partitions does not increase automatically. If needed, you can re-enable autopartitioning for this topic.

<<<<<<< HEAD
Examples of YQL queries for switching between different autopartitioning strategies can be found [here](../../yql/reference/syntax/alter-topic.md#autopartitioning).
=======
Examples of YQL queries for switching the topic to various auto-partitioning modes can be found [here](../../yql/reference/syntax/alter-topic.md#autopartitioning).
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Autopartitioning Constraints {#autopartitioning_constraints}

<<<<<<< HEAD
The following constraints apply when using autopartitioning:

1. Once autopartitioning is enabled for a topic, it cannot be stopped, only paused.
2. When autopartitioning is enabled for a topic, it is impossible to read from or write to it using the [Kafka API](../../reference/kafka-api/index.md).
3. Autopartitioning can only be enabled on topics that use the reserved capacity mode.
=======
The following limitations apply when using auto-partitioning:

1. If auto-partitioning is enabled on a topic, it cannot be disabled, only paused.
2. If auto-partitioning is enabled on a topic, writing to or reading from such a topic via the [Kafka API protocol](../../reference/kafka-api/index.md) is not possible.
3. Auto-partitioning cannot be enabled on a topic with the local storage mode.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

## Message Sources {#producer-id}

Messages are ordered using the `producer_id`. The order of written messages is maintained within `producer_id`.

<<<<<<< HEAD
When used for the first time, a `producer_id` is linked to a topic's [partition](#partitioning) using the round-robin algorithm and all messages with `producer_id` get into the same partition. The link is removed if there are no new messages using this producer ID for 14 days.
=======
On first use, the source identifier `producer_id` is bound to a topic [partition](#partitioning) using a round-robin algorithm, and all messages with this `producer_id` end up in the same partition. The binding is removed if no new messages using this source identifier appear within 14 days.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

{% note warning %}

The recommended maximum number of `producer_id` pairs is up to 100 thousand per partition in the last 14 days.

{% endnote %}

<<<<<<< HEAD
{% cut "Why and When the Message Processing Order Is Important" %}

### When the Message Processing Order Is Important
=======
### When message processing order matters

Consider a financial application whose task is to calculate the user's account balance and allow or deny debiting funds.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

Let's consider a finance application that calculates the balance on a user's account and permits or prohibits debiting the funds.

<<<<<<< HEAD
For such tasks, you can use a [message queue](https://en.wikipedia.org/wiki/Message_queue). When you top up your account, debit funds, or make a purchase, a message with the account ID, amount, and transaction type is registered in the queue. The application processes incoming messages and calculates the balance.

![basic-design](../../_assets/example-basic-design.svg)

To accurately calculate the balance, the message processing order is crucial. If a user first tops up their account and then makes a purchase, messages with details about these transactions must be processed by the app in the same order. Otherwise, there may be an error in the business logic and the app will reject the purchase as a result of insufficient funds. There are guaranteed delivery order mechanisms, but they cannot ensure a message order within a single queue on an arbitrary data amount.

When several application instances read messages from a stream, a message about account top-ups can be received by one instance and a message about debiting by another. In this case, there's no guaranteed instance with accurate balance information. To avoid this issue, you can, for example, save data in the DBMS, share information between application instances, and implement a distributed cache.
=======
![basic-design](../../_assets/example-basic-design.svg)

For correct balance calculation, the order of message processing is important. If a user first deposits money and then makes a purchase, the messages with information about these operations must be processed by the application in the same sequence. Otherwise, a business logic error may occur, and, for example, the application will reject the purchase due to insufficient funds. Message queues have mechanisms for guaranteed delivery order, but they cannot ensure the order of messages within a single queue for arbitrary data volumes.

When multiple application instances read messages from a stream, one instance may receive the deposit message and another the withdrawal message. In this case, there is no instance that is guaranteed to have the correct balance information. To solve this problem, you can store data in a DBMS, exchange information between application instances, build a distributed cache, etc.

In {{ ydb-short-name }}, you can write data so that messages from one source arrive at the same application instance. To do this, messages from each source are written with their unique source identifiers (`producer_id`), and a message sequence number from the source (`seqno`) is used to protect against duplicates. In {{ ydb-short-name }}, messages with the same `producer_id` end up in the same partition. When reading from a topic, each reader instance serves its own subset of partitions, thus eliminating the need for synchronization between instances. For example, using this approach, you can ensure that messages about transactions for the same account always end up in the same partition and are processed by the application instance associated with that partition.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

{{ ydb-short-name }} can write data so that messages from a single source are delivered to the same application instance. Each source writes messages with its own unique `producer_id`, and a sequence number (`seqno`) is used to prevent duplicate processing. {{ ydb-short-name }} routes all messages with the same `producer_id` to the same partition. When reading from a topic, each reader instance handles its own subset of partitions, eliminating the need for synchronization between instances. For example, this approach could allow all transactions from a given account to be processed by the application instance associated with it.

<<<<<<< HEAD
Below is an example when all transactions on accounts with even IDs are transferred to the first instance of the application, and with odd ones — to the second.
=======
![topic-design](../../_assets/example-topic-design.svg)
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

![topic-design](../../_assets/example-topic-design.svg)

<<<<<<< HEAD
### When the Processing Order Is Not Important {#no-dedup}
=======
For some tasks, the order of message processing is not critical. For example, sometimes it is important just to deliver the data, and the storage system will handle ordering.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

For some tasks, the message processing order is not critical. For example, it's sometimes important to simply deliver data that will then be ordered by the storage system.

For such tasks, the "no-deduplication" mode can be used. In this scenario, [`producer_id`](#producer-id) isn't specified in the write session setup, and [`sequence numbers`](#seqno) aren't used for messages. The no-deduplication mode offers better performance and requires fewer server resources; however, there is no message ordering or deduplication on the server side. This means that a message sent to the server multiple times (for example, due to network instability or a writer process crash) may be written to the topic multiple times.

<<<<<<< HEAD
{% endcut %}
=======
All messages from one source have a sequence number, [`sequence number`](#seqno), used for deduplication. The message sequence number must increase monotonically within the pair `topic`, `source`. When the server receives a message with a sequence number less than or equal to the maximum recorded for the pair `topic`, `source`, the message will be skipped as a duplicate. Gaps in the sequence of message numbers are allowed. Message sequence numbers must be unique only within the pair `topic`, `source`.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

## Message Sequence Numbers {#seqno}

All messages from the same source have a [`sequence number`](#seqno) used for their deduplication. A message sequence number should monotonically increase within a `topic`, `source` pair. If the server receives a message whose sequence number is less than or equal to the maximum number written for the `topic`, `source` pair, the message will be skipped as a duplicate. Some sequence numbers in the sequence may be skipped. Message sequence numbers must be unique within the `topic`, `source` pair.

<<<<<<< HEAD
Sequence numbers are not used if [no-deduplication mode](#no-dedup) is enabled.
=======
| Type | Example | Description |
| --- | --- | --- |
| File | Offset of transmitted data from the beginning of the file | You cannot delete rows from the beginning of the file, as this will lead to skipping part of the data, duplicates, or loss of part of the data. |
| Database table | Auto-increment record identifier |  |
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Sample Message Sequence Numbers {#seqno-examples}

<<<<<<< HEAD
| Type     | Example | Description                                                                                                                        |
|----------| --- |------------------------------------------------------------------------------------------------------------------------------------|
| File     | Offset of transferred data from the beginning of a file | You can't delete lines from the beginning of a file, since this will lead to skipping some data as duplicates or losing some data. |
| DB table | Auto-increment record ID | |
=======
Each topic has a defined message retention time. After the retention time expires, messages are automatically deleted. An exception is data that has not yet been acknowledged by an ["important"](#important-consumer) reader — it will be stored until the reader processes it. If there is a reader with an explicitly specified [availability time](#availability-period-consumer), the retention time of unprocessed messages is increased to the specified value.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

## Message Retention Period {#retention-time}

<<<<<<< HEAD
The message retention period is set for each topic. After it expires, messages are automatically deleted. An exception is data that hasn't been committed by an [important](#important-consumer) consumer: this data will be stored until it's read.
=======
During transmission, the writer application indicates that the message can be compressed using one of the supported codecs. The codec name is passed during writing and stored with the message, and is also returned on reading. Message compression is performed individually for each message; batch compression is not supported. Data compression and decompression operations are performed on the reader and writer application side.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

## Data Compression {#message-codec}

When transferring data, the producer app indicates that a message can be compressed using one of the supported codecs. The codec name is passed while writing a message, saved along with it, and returned when reading the message. Compression applies to each individual message, no batch message compression is supported. Data is compressed and decompressed on the producer and consumer apps' end.

Supported codecs are explicitly listed in each topic. When making an attempt to write data to a topic with a codec that is not supported, a write error occurs.

| Codec  | Description                                             |
|--------|---------------------------------------------------------|
| `raw`  | No compression.                                         |
| `gzip` | [Gzip](https://en.wikipedia.org/wiki/Gzip) compression. |
{% if audience != "external" %}
<<<<<<< HEAD
| `lzop` | [lzop](https://en.wikipedia.org/wiki/Lzop) compression. |
=======

| | Compression using the [`lzop`](https://en.wikipedia.org/wiki/Lzop) algorithm. |

>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
{% endif %}
| `zstd` | [zstd](https://en.wikipedia.org/wiki/Zstd) compression. |

<<<<<<< HEAD
## Consumer {#consumer}
=======
| | Compression using the [`zstd`](https://en.wikipedia.org/wiki/Zstd) algorithm. |
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

A consumer is a named entity that reads data from a topic. A consumer contains committed consumer offsets for each topic read on their behalf.

### Consumer Offset {#consumer-offset}

<<<<<<< HEAD
A consumer offset is a saved [offset](#offset) of a consumer by each topic partition. It's saved by a consumer after sending commits of the data read. When a new read session is established, messages are delivered to the consumer starting with the saved consumer offset. This lets users avoid saving the consumer offset on their end.
=======
A read session is a client connection to a topic to receive messages on behalf of a reader. Read sessions work through the Topic API. A single reader can establish multiple read sessions: in this case, topic partitions are distributed among these sessions. To work with read sessions, it is recommended to use the {{ ydb-short-name }} SDK (see [Working with topics](../../reference/ydb-sdk/topic.md)).
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Important Consumer {#important-consumer}

<<<<<<< HEAD
A consumer may be flagged as "important". This flag indicates that messages in a topic won't be removed until the consumer reads and confirms them. You can set this flag for most critical consumers that need to handle all data even if there's a long idle time.

{% note warning %}

As a long timeout of an important consumer may result in full use of all available free space by unread messages, be sure to monitor important consumers' data read lags.

{% endnote %}

## Topic Protocols {#topic-protocols}

To work with topics, the {{ ydb-short-name }} SDK is used (see also [Reference](../../reference/ydb-sdk/topic.md)).

Kafka API version 3.4.0 is also supported with some restrictions (see [Work with Kafka API](../../reference/kafka-api/index.md)).

## Transactions with Topics {#topic-transactions}
=======
- [**streaming**](#streaming-consumer) — a reader type where a partition is exclusively assigned to one consumer, and all messages from that partition are processed by that consumer.
- [**shared (common)**](#shared-consumer) — a reader type that allows multiple consumers to jointly process messages from a single topic without exclusive assignment of partitions to each of them.

## Streaming reader {#streaming-consumer}

A streaming reader is designed for streaming data processing from a topic. In this mode, multiple instances of the consumer application read messages in parallel and process them as they arrive. Typical scenarios include collecting and analyzing application logs, building real-time aggregates, and replicating events to external systems.

You can read from a topic using a streaming reader via the [Topic API](../../reference/ydb-sdk/topic.md) and the [Kafka](../../reference/kafka-api/index.md) protocol.

In streaming mode, each [partition](#partitioning) of a topic is assigned to at most one consumer at any given time. All messages from the assigned partition are processed by that consumer.

The {{ ydb-short-name }} server is responsible for distributing partitions among consumers. When consumers connect and disconnect, the server redistributes partitions and aims to distribute them as evenly as possible among active consumers.

The number of partitions determines the maximum degree of parallelism: no more consumers than partitions can read from a topic simultaneously. If there are more consumers than partitions, some instances remain without assigned partitions and are idle until free partitions become available or the number of consumers decreases.

## Shared (common) reader {#shared-consumer}

A shared (common) reader in {{ ydb-short-name }} is a reading model from a topic where one or more messages from the topic are assigned to the reader, rather than an entire topic partition. This allows a single partition to be processed simultaneously by a large number of consumers.

A typical use case is message exchange between microservices. Reading from a topic using a shared (common) reader is possible via the [Amazon SQS](../../reference/sqs-api/index.md) protocol.

A message is locked for a consumer for a certain period of time. If during this time the consumer has not reported completion of message processing and has not extended its processing time, it means that the consumer could not process the message, and the message becomes available for reading and will be read again. The processing time is configured in the reader parameters, but can also be set for each message read request.

A Dead letter policy is applied to messages that cannot be processed. The Dead letter policy can be configured in one of the following ways:

- **none** — the message is returned to the queue after unsuccessful processing and will later be given for re-reading.
- **move** — the message is moved to a DLQ after several processing attempts; any other database topic can be specified as the DLQ.
- **delete** — the message is deleted after several processing attempts.

By default, the Dead letter policy is disabled (none). The number of message processing attempts before moving it to a DLQ (move) or deleting it (delete) is configured on the reader. If the message cannot be moved to the DLQ, it is returned to the queue (as when the Dead letter policy is disabled), and on the next unsuccessful attempt to process the message, another attempt will be made to move the message to the DLQ. For example, if you delete the topic specified as the DLQ, messages after unsuccessful processing will be returned to the queue until the topic specified as the DLQ is created or another existing topic is specified.

Depending on the configuration of the shared (common) reader, reading from a topic can be performed either with or without preserving message order. Message order is preserved among messages with the same message-group-id, which can be set for each message when it is written. If the shared (common) reader is configured to read with message order preservation, then at any given time, such a reader delivers no more than one message from a single message-group-id to consumers for processing.

When placing a message in the queue, you can specify a delay before the message is given for processing for the first time. Message order is determined by the time the message was added to the topic — if reading is performed with message order preservation, a delayed message will pause the delivery for processing of all subsequent messages with the same message-group-id.

See also [{#T}](../../dev/shared-consumer-internals.md).

### Reading position {#consumer-offset}

The reading position is the saved [offset](#offset) of the reader for each partition of the topic. The reading position is saved by the reader after sending an acknowledgment of the read data. When establishing a new reading session, messages are delivered to the reader starting from the saved reading position. This allows users not to store the reading position on their side.

### Reading limit from a single partition {#partition-max-in-flight-bytes}

The `partition_max_in_flight_bytes` parameter can be set when creating a reading session.

This parameter sets the upper limit of in-flight - the amount of data for a single partition that has already been issued by the server in read responses but not yet confirmed by an offset commit.

- `0` — no separate in-flight limit per partition is applied.
- positive value — when the limit is reached, the server temporarily stops delivering new portions for this partition until the client performs a commit and reduces in-flight.

A value that is too small, with slow processing or infrequent commits, can cause reading pauses on a partition. In such a case, increase the limit or perform commits more often.

### Important reader {#important-consumer}

A reader can have the "important" attribute. The presence of this attribute means that messages in the topic will not be deleted until the reader reads and confirms the messages. This attribute can be set for the most critical readers that must process all data even during long downtime.

{% note warning %}

Since a prolonged idle period of an important reader can lead to using all available data storage space with unread messages, it is necessary to monitor the read lag of important readers.

{% endnote %}

### Message availability time for the reader {#availability-period-consumer}

A time availability period can be set for the reader, during which messages from the topic that have not yet been processed will be available to them.

This option allows extending the message retention time in the topic from the [retention time](#retention-time) up to the specified availability time, if the reader does not acknowledge processing.
Unlike the ["important"](#important-consumer) reader attribute, this parameter limits the maximum age of messages that will be stored in the topic.
If the option is not set or all readers acknowledge processing without significant delay (less than the topic's [retention time](#retention-time)), then data will be deleted according to the usual rules.

## Protocols for working with topics {#topic-protocols}

The {{ ydb-short-name }} SDK is used for working with topics (see [Working with topics](../../reference/ydb-sdk/topic.md)).

Also, the Kafka API version 3.4.0 (see [{#T}](../../reference/kafka-api/index.md)) and Amazon SQS API (see [{#T}](../../reference/sqs-api/index.md)) protocols are partially supported.

Work with a single topic can be carried out simultaneously using multiple protocols. For example, writing can be done using the Topic API, and reading using the Amazon SQS API, and vice versa.

## Transactions involving topics {#topic-transactions}
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

{{ ydb-short-name }} supports working with topics within [transactions](../transactions.md).

### Read from a Topic Within a Transaction {#topic-transactions-read}

<<<<<<< HEAD
Topic data does not change during a read operation. Therefore, within transactional reads from a topic, only the offset commit is a true transactional operation. The postponed offset commit occurs automatically at the transaction commit, and the SDK handles this transparently for the user.
=======
Data in topics is not modified when reading from a topic. Therefore, when reading from a topic in a transaction, the only transactional operation is changing the offset. When reading transactionally via the SDK, offsets are not committed. Deferred offset commit occurs automatically when the transaction is committed; the SDK hides this from the user.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Write into a Topic Within a Transaction {#topic-transactions-write}

<<<<<<< HEAD
During transactional writes to a topic, data is stored outside the partition until the transaction is committed. At the transaction commit, the data is published to the partition and appended to the end of the partition with sequential offsets. Changes made within the transaction are not visible in transactions with topics in {{ ydb-short-name }}.
=======
When writing transactionally to a topic, data is stored outside the partition before commit, and then published (becomes visible) at the moment of transaction commit. In this case, data will be added to the end of the partition at sequential offsets. Visibility of own changes in topics within transactions involving topics is not provided.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

### Topic Transaction Constraints {#topic-transactions-constraints}

There are no additional constraints when working with topics within a transaction. It is possible to write large amounts of data to a topic, write to multiple partitions, and read with multiple consumers.

<<<<<<< HEAD
However, it is recommended to consider that data is published only at transaction commit. Therefore, if a transaction is long-running, the data will become visible only after a significant delay.
=======
Nevertheless, it is recommended to choose the transaction mode taking into account the specifics of transactional work with topics: data is published at the moment of transaction commit. That is, if the transaction is long-running, data will become visible only after a significant amount of time.

## YQL queries to topics {#yql-topics}

To work with topics, you can use [YQL queries](../query_execution/topics.md) with familiar constructs: [SELECT](../../yql/reference/syntax/select/index.md) for reading messages from a topic and [INSERT](../../yql/reference/syntax/insert_into.md) for writing messages to a topic.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
