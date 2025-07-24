# Topic

A topic in {{ ydb-short-name }} is an entity for storing unstructured messages and delivering them to multiple subscribers. Basically, a topic is a named set of messages.

A producer app writes messages to a topic. Consumer apps are independent of each other, they receive and read messages from the topic in the order they were written there. Topics implement the [publish-subscribe](https://en.wikipedia.org/wiki/Publish–subscribe_pattern) architectural pattern.

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

 - **Active.** By default, all partitions are active. Both read and write operations are allowed on an active partition.
 - **Inactive.** An inactive partition is read-only. A partition becomes inactive after splitting for [autopartitioning](#autopartitioning). It is automatically deleted once all messages are removed due to the expiration of the retention period.

### Offset {#offset}

All messages within a partition have a unique sequence number called an `offset`. An offset monotonically increases as new messages are written.

## Autopartitioning {#autopartitioning}

Total topic throughput is determined by the number of partitions in the topic and the throughput of each partition. The number of partitions and the throughput of each partition are set at the time of topic creation. If the maximum required write speed for a topic is unknown at the creation time, autopartitioning allows the topic to be scaled automatically. If autopartitioning is enabled for a topic, the number of partitions will increase automatically as the write speed increases (see [Autopartitioning Strategies](#autopartitioning_strategies)).

### Guarantees {#autopartitioning_guarantee}

1. The SDK and server provide an exactly-once guarantee in the case of writing during a partition split. This means that any message will be written either to the parent partition or to one of the child partitions but never to both simultaneously. Additionally, a message cannot be written to the same partition multiple times.
2. The SDK and server maintain the reading order. Data is read from the parent partition first, followed by the child partitions.
3. As a result, the exactly-once writing guarantee and the reading order guarantee are preserved for a specific [producer identifier](#producer-id).

### Autopartitioning Strategies {#autopartitioning_strategies}

The following autopartitioning strategies are available for any topic:

#### DISABLED

Autopartitioning is disabled for this topic. The number of partitions remains constant, and there is no automatic scaling.

The initial number of partitions is set during topic creation. If the partition count is manually adjusted, new partitions are added. Both previously existing and new partitions are active.

#### UP

Upwards autopartitioning is enabled for this topic. This means that if the write speed to the topic increases, the number of partitions will automatically increase. However, if the write speed decreases, the number of partitions remains unchanged.

The partition count increase algorithm works as follows: if the write speed for a partition exceeds a defined threshold (as a percentage of the maximum write speed for that partition) during a specified period, the partition is split into two child partitions. The original partition becomes inactive, allowing only read operations. When the retention period expires, and all messages in the original partition are deleted, the partition itself is also deleted. The two new child partitions become active, allowing both read and write operations.

#### PAUSED

Autopartitioning is paused for this topic, meaning that the number of partitions does not increase automatically. If needed, you can re-enable autopartitioning for this topic.

Examples of YQL queries for switching between different autopartitioning strategies can be found [here](../yql/reference/syntax/alter-topic.md#autopartitioning).

### Autopartitioning Constraints {#autopartitioning_constraints}

The following constraints apply when using autopartitioning:

1. Once autopartitioning is enabled for a topic, it cannot be stopped, only paused.
2. When autopartitioning is enabled for a topic, it is impossible to read from or write to it using the [Kafka API](../reference/kafka-api/index.md).
3. Autopartitioning can only be enabled on topics that use the reserved capacity mode.

## Message Sources {#producer-id}

Messages are ordered using the `producer_id`. The order of written messages is maintained within `producer_id`.

When used for the first time, a `producer_id` is linked to a topic's [partition](#partitioning) using the round-robin algorithm and all messages with `producer_id` get into the same partition. The link is removed if there are no new messages using this producer ID for 14 days.

{% note warning %}

The recommended maximum number of `producer_id` pairs is up to 100 thousand per partition in the last 14 days.

{% endnote %}

{% cut "Why and When the Message Processing Order Is Important" %}

### When the Message Processing Order Is Important

Let's consider a finance application that calculates the balance on a user's account and permits or prohibits debiting the funds.

For such tasks, you can use a [message queue](https://en.wikipedia.org/wiki/Message_queue). When you top up your account, debit funds, or make a purchase, a message with the account ID, amount, and transaction type is registered in the queue. The application processes incoming messages and calculates the balance.

![basic-design](../_assets/example-basic-design.svg)

To accurately calculate the balance, the message processing order is crucial. If a user first tops up their account and then makes a purchase, messages with details about these transactions must be processed by the app in the same order. Otherwise, there may be an error in the business logic and the app will reject the purchase as a result of insufficient funds. There are guaranteed delivery order mechanisms, but they cannot ensure a message order within a single queue on an arbitrary data amount.

When several application instances read messages from a stream, a message about account top-ups can be received by one instance and a message about debiting by another. In this case, there's no guaranteed instance with accurate balance information. To avoid this issue, you can, for example, save data in the DBMS, share information between application instances, and implement a distributed cache.

{{ ydb-short-name }} can write data so that messages from a single source are delivered to the same application instance. Each source writes messages with its own unique `producer_id`, and a sequence number (`seqno`) is used to prevent duplicate processing. {{ ydb-short-name }} routes all messages with the same `producer_id` to the same partition. When reading from a topic, each reader instance handles its own subset of partitions, eliminating the need for synchronization between instances. For example, this approach could allow all transactions from a given account to be processed by the application instance associated with it. 

Below is an example when all transactions on accounts with even IDs are transferred to the first instance of the application, and with odd ones — to the second.

![topic-design](../_assets/example-topic-design.svg)

### When the Processing Order Is Not Important {#no-dedup}

For some tasks, the message processing order is not critical. For example, it's sometimes important to simply deliver data that will then be ordered by the storage system.

For such tasks, the "no-deduplication" mode can be used. In this scenario, [`producer_id`](#producer-id) isn't specified in the write session setup, and [`sequence numbers`](#seqno) aren't used for messages. The no-deduplication mode offers better performance and requires fewer server resources; however, there is no message ordering or deduplication on the server side. This means that a message sent to the server multiple times (for example, due to network instability or a writer process crash) may be written to the topic multiple times.

{% endcut %}

## Message Sequence Numbers {#seqno}

All messages from the same source have a [`sequence number`](#seqno) used for their deduplication. A message sequence number should monotonically increase within a `topic`, `source` pair. If the server receives a message whose sequence number is less than or equal to the maximum number written for the `topic`, `source` pair, the message will be skipped as a duplicate. Some sequence numbers in the sequence may be skipped. Message sequence numbers must be unique within the `topic`, `source` pair.

Sequence numbers are not used if [no-deduplication mode](#no-dedup) is enabled.

### Sample Message Sequence Numbers {#seqno-examples}

| Type     | Example | Description                                                                                                                        |
|----------| --- |------------------------------------------------------------------------------------------------------------------------------------|
| File     | Offset of transferred data from the beginning of a file | You can't delete lines from the beginning of a file, since this will lead to skipping some data as duplicates or losing some data. |
| DB table | Auto-increment record ID | |

## Message Retention Period {#retention-time}

The message retention period is set for each topic. After it expires, messages are automatically deleted. An exception is data that hasn't been committed by an [important](#important-consumer) consumer: this data will be stored until it's read.

## Data Compression {#message-codec}

When transferring data, the producer app indicates that a message can be compressed using one of the supported codecs. The codec name is passed while writing a message, saved along with it, and returned when reading the message. Compression applies to each individual message, no batch message compression is supported. Data is compressed and decompressed on the producer and consumer apps' end.

Supported codecs are explicitly listed in each topic. When making an attempt to write data to a topic with a codec that is not supported, a write error occurs.

| Codec  | Description                                             |
|--------|---------------------------------------------------------|
| `raw`  | No compression.                                         |
| `gzip` | [Gzip](https://en.wikipedia.org/wiki/Gzip) compression. |
{% if audience != "external" %}
| `lzop` | [lzop](https://en.wikipedia.org/wiki/Lzop) compression. |
{% endif %}
| `zstd` | [zstd](https://en.wikipedia.org/wiki/Zstd) compression. |

## Consumer {#consumer}

A consumer is a named entity that reads data from a topic. A consumer contains committed consumer offsets for each topic read on their behalf.

### Consumer Offset {#consumer-offset}

A consumer offset is a saved [offset](#offset) of a consumer by each topic partition. It's saved by a consumer after sending commits of the data read. When a new read session is established, messages are delivered to the consumer starting with the saved consumer offset. This lets users avoid saving the consumer offset on their end.

### Important Consumer {#important-consumer}

A consumer may be flagged as "important". This flag indicates that messages in a topic won't be removed until the consumer reads and confirms them. You can set this flag for most critical consumers that need to handle all data even if there's a long idle time.

{% note warning %}

As a long timeout of an important consumer may result in full use of all available free space by unread messages, be sure to monitor important consumers' data read lags.

{% endnote %}

## Topic Protocols {#topic-protocols}

To work with topics, the {{ ydb-short-name }} SDK is used (see also [Reference](../reference/ydb-sdk/topic.md)).

Kafka API version 3.4.0 is also supported with some restrictions (see [Work with Kafka API](../reference/kafka-api/index.md)).

## Transactions with Topics {#topic-transactions}

{{ ydb-short-name }} supports working with topics within [transactions](./transactions.md).

### Read from a Topic Within a Transaction {#topic-transactions-read}

Topic data does not change during a read operation. Therefore, within transactional reads from a topic, only the offset commit is a true transactional operation. The postponed offset commit occurs automatically at the transaction commit, and the SDK handles this transparently for the user.

### Write into a Topic Within a Transaction {#topic-transactions-write}

During transactional writes to a topic, data is stored outside the partition until the transaction is committed. At the transaction commit, the data is published to the partition and appended to the end of the partition with sequential offsets. Changes made within the transaction are not visible in transactions with topics in {{ ydb-short-name }}.

### Topic Transaction Constraints {#topic-transactions-constraints}

There are no additional constraints when working with topics within a transaction. It is possible to write large amounts of data to a topic, write to multiple partitions, and read with multiple consumers.

However, it is recommended to consider that data is published only at transaction commit. Therefore, if a transaction is long-running, the data will become visible only after a significant delay.
