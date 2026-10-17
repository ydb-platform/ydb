# Working with topics

This article provides examples of using the {{ ydb-short-name }} SDK to work with [topics](../../concepts/datamodel/topic.md).

The snippets below are parts of the complete executable examples linked in the next section. The applications create uniquely named topics and consumers and remove them after completing the scenarios. Setup and helper functions are included in those applications.

## Examples of working with topics

{% list tabs group=lang %}

- C++

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

- Go

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

- Java

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

- Python

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

- C#

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/783cfd6c1803bbd607577c5b59c4ae0cc9a5d9a7/examples/ydb_tech/topic)

- JavaScript

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-js-sdk/tree/e107f68250200a6ddfaa6074ccd1ec48ed1ae839/examples/ydb_tech/topic)

- Rust

  [Executable example on GitHub](https://github.com/ydb-platform/ydb-rs-sdk/tree/041636b79c6d607c6ce2e594b28bb510db514680/ydb/examples/ydb_tech/topic)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Initializing a connection to topics {#init}

{% list tabs group=lang %}

- C++

  To work with topics, instances of the {{ ydb-short-name }} driver and client are created.

  The {{ ydb-short-name }} driver is responsible for the interaction between the application and {{ ydb-short-name }} at the transport level. The driver must exist throughout the entire lifecycle of working with topics and must be initialized before creating the client.

  The topic service client ( [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) runs on top of the {{ ydb-short-name }} driver and is responsible for management operations with topics, as well as creating read and write sessions.

  Application code snippet for initializing the {{ ydb-short-name }} driver:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_init]-[END topic_init]" %}

  This example uses an authentication token stored in the `YDB_TOKEN` environment variable. For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  Application code snippet for creating a client:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_client]-[END topic_client]" %}

- Go

  To work with topics, an instance of the {{ ydb-short-name }} driver created using `ydb.Open` is used. The topic client is available via the `db.Topic()` method.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_init]-[END topic_init]" %}

- Java

  To work with topics, instances of the {{ ydb-short-name }} transport and client are created.

  The {{ ydb-short-name }} transport is responsible for the interaction between the application and {{ ydb-short-name }} at the transport level. It must exist throughout the entire lifecycle of working with topics and must be initialized before creating the client.

  Application code snippet for initializing the {{ ydb-short-name }} transport:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_init]-[END topic_init]" %}

  This example uses the helper method `CloudAuthHelper.getAuthProviderFromEnviron()`, which obtains a token from environment variables.
  For example, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  The topic service client ( [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) runs on top of the {{ ydb-short-name }} transport and is responsible for both management operations with topics and creating writers and readers.

  Application code snippet for creating a client:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_client]-[END topic_client]" %}

  Both code examples above use a ( [try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)) block.
  This allows automatically closing the client and transport when exiting this block, as both are descendants of `AutoCloseable`.
- C#

  To work with topics, simply pass the connection string directly to the constructor of the required client.

  This example uses anonymous authentication. For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  Application code snippet for creating various topic clients:

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_init]-[END topic_init]" %}

- Python

  To work with topics, an instance of the {{ ydb-short-name }} driver is created. The topic client is available via the `topic_client` attribute and is used for management operations with topics, as well as creating writers and readers.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_init]-[END topic_init]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_init]-[END topic_init]" %}

  {% endlist %}

  For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).
- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_init]-[END topic_init]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_init]-[END topic_init]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Managing topics {#manage}

### Creating a topic {#create-topic}

The only required parameter for creating a topic is its path; the other parameters are optional.

{% list tabs group=lang %}

- C++

  The full list of settings can be found [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

  Example of creating a topic with three partitions and ZSTD codec support:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_create]-[END topic_create]" %}

- Go

  The full list of supported parameters can be found in the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_create]-[END topic_create]" %}

- Python

  Example of creating a topic with a list of supported codecs and a minimum number of partitions

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_create]-[END topic_create]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_create]-[END topic_create]" %}

  {% endlist %}
- Java

  The full list of settings can be found [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_create]-[END topic_create]" %}

- C#

  Example of creating a topic with a list of supported codecs and a minimum number of partitions:

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_create]-[END topic_create]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_create]-[END topic_create]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_create]-[END topic_create]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Modifying a topic {#alter-topic}

{% list tabs group=lang %}

- C++

  When changing a topic, in the parameters of the `AlterTopic` method you need to specify the topic path and the parameters to be changed. The changed parameters are represented by the `TAlterTopicSettings` structure.

  You can see the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  Example of adding an [important reader](../../concepts/datamodel/topic#important-consumer) to a topic and setting the [message retention time](../../concepts/datamodel/topic#retention-time) for the topic to two days:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Go

  When changing a topic, in the parameters you need to specify the topic path and the parameters to be changed.

  You can see the full list of supported parameters in the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

  Example of adding a reader to a topic

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Python

  Example of changing the list of supported codecs and the minimum number of partitions for a topic

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_alter]-[END topic_alter]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_alter]-[END topic_alter]" %}

  {% endlist %}
- Java

  When changing a topic, in the parameters of the `alterTopic` method you need to specify the topic path and the parameters to be changed.

  You can see the full list of settings [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/AlterTopicSettings.java#L23).

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_alter]-[END topic_alter]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Getting information about a topic {#describe-topic}

{% list tabs group=lang %}

- C++

  To get information about a topic, use the `DescribeTopic` method.

  The topic description is represented by the `TTopicDescription` structure.

  See the full list of description fields [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L163).

  You can access this description as follows:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  There is a separate method for getting information about a reader - `DescribeConsumer`.
- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_describe]-[END topic_describe]" %}

  {% endlist %}
- Java

  To get information about a topic, use the `describeTopic` method.

  You can see the full list of description fields [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_describe]-[END topic_describe]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Deleting a topic {#drop-topic}

To delete a topic, just specify its path.

{% list tabs group=lang %}

- C++

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_drop]-[END topic_drop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_drop]-[END topic_drop]" %}

  {% endlist %}
- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_drop]-[END topic_drop]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Writing messages {#write}

### Connecting to a topic for writing messages {#start-writer}

Currently, only connections with matching [source and message group](../../concepts/datamodel/topic#producer-id) identifiers (`producer_id` and `message_group_id`) are supported; this limitation will be removed in the future.

{% list tabs group=lang %}

- C++

  In the C++ SDK, three API options are available for writing to a topic. The basic settings (buffering, codecs, retries) are the same for all three and are set by the `TWriteSessionSettings` structure, so the tips below focus on differences and usage scenarios.

  - `IWriteSession` — a low-level write session to a single partition with a full set of features: event loop (`TReadyToAcceptEvent`, `TAcksEvent`, `TSessionClosedEvent`), explicit send flow control via `TContinuationToken` that the system issues before receiving each message; independent acknowledgments for each message; sending pre-compressed data via the `WriteEncoded` method, bypassing recompression on the server. The `TWriteSessionSettings` structure is defined here — the other two options reuse it. Suitable when you need the status of each message, custom asynchronous logic, or manual compression control.
  - `ISimpleBlockingWriteSession` — a synchronous fire-and-forget API for writing to a single topic partition, the simplest option. The `Write(message, blockTimeout)` method puts the message into an internal buffer, and sending to the server happens in the background. In normal mode, the call returns instantly and blocks only when the buffer is full (by `MaxMemoryUsage` / `MaxInflightCount`), for no longer than `blockTimeout`. A return of `false` means the message **did not get into the buffer** and is lost. There are no acknowledgments for individual messages; you can only verify that the entire buffer has been delivered to the server by calling `Close()` — it waits for an ack from the server. Suitable when an "all or nothing" guarantee by session close is sufficient and simple synchronous code is needed.
  - `IProducer` — a high-level API on top of multiple write sessions: transparently shards messages across topic partitions by key. Inspired by the Producer interface from Apache Kafka, but takes into account the specifics of {{ ydb-short-name }} and, when working with topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning), provides full ordering and exactly-once guarantees. Acknowledgments from the server are available via the `AcksHandler` handler; to wait for delivery of the accumulated buffer — `Flush()`, to shut down correctly — `Close()`. Suitable when you need to write to a **multi-partition** topic with key-based routing.

  {% list tabs %}

  - IWriteSession

    `IWriteSession` — a basic write session from which other write options inherit settings. The write session settings are represented by the `TWriteSessionSettings` structure; for the `ISimpleBlockingWriteSession` option, some settings are not supported.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  - ISimpleBlockingWriteSession

    `ISimpleBlockingWriteSession` is a simple synchronous variant of the `IWriteSession` write session for writing one message at a time without acknowledgment for each message. The `Write` method blocks when the number of inflight records or the SDK buffer size is exceeded. Write session settings are represented by the `TWriteSessionSettings` structure, as in the case of `IWriteSession`, however some settings are not supported.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_writer_blocking]-[END topic_start_writer_blocking]" %}

  - IProducer

    `IProducer` is a high-level API over write sessions: a single object hides the management of multiple sessions and automatically selects a partition based on the message key. Producer settings are represented by the `TProducerSettings` structure, which inherits from `TWriteSessionSettings`, so the common write settings match those of `IWriteSession`.

    Settings are specified via `TProducerSettings`:

    - `ProducerIdPrefix` — producer id prefix for write sub-sessions.
    - `PartitionChooserStrategy` — partition selection strategy by message key:

      - `Bound` — the key is matched against the topic partition ranges (`FromBound`/`ToBound` from the topic description). By default, before matching, the key is passed through MurmurHash64. Recommended for topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning): when a partition splits, the SDK updates the boundaries and continues to route messages with the same key to the correct range.
      - `KafkaHash` — analogous to Kafka: MurmurHash is computed from the key, the partition index is the remainder of dividing the hash by the number of partitions. Convenient when migrating from Kafka. Not supported when auto-partitioning is enabled.
    - `PartitioningKeyHasher` — key transformation function before matching against ranges; used only for the `Bound` strategy. You can set your own, for example to have the original key participate in the comparison without hashing.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h#L10).

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_producer]-[END topic_start_producer]" %}

  {% endlist %}
- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Initializing writer settings:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_writer_settings]-[END topic_writer_settings]" %}

    Creating a synchronous writer:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer]-[END topic_sync_writer]" %}

    After creating the writer, it must be initialized. There are two methods for this:

    - `init()`: non-blocking, starts the initialization process in the background and does not wait for it to complete.

      {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer_init]-[END topic_sync_writer_init]" %}

    - `initAndWait()`: blocking, starts the initialization process and waits for it to complete. If an error occurs during initialization, an exception is thrown.

      {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_writer_init_wait]-[END topic_sync_writer_init_wait]" %}

  - Asynchronous API

    Initializing writer settings:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_writer_settings]-[END topic_writer_settings]" %}

    Creating and initializing an asynchronous writer:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_async_writer]-[END topic_async_writer]" %}

  {% endlist %}
- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_start_writer]-[END topic_start_writer]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages {#writing-messages}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Working with the `IWriteSession` object is structured as event loop processing with three event types: `TReadyToAcceptEvent`, `TAcksEvent`, and `TSessionClosedEvent`.

    For each event type, you can set a handler for that event, and you can also set a common handler. Handlers are set in the write session settings before its creation.

    If a handler for a certain event is not set, you must obtain and process it in the `GetEvent` / `GetEvents` methods. For non-blocking waiting for the next event, there is the `WaitEvent` method with the `TFuture<void>()` interface.

    To write each message, the user must "spend" a move-only object `TContinuationToken`, which the SDK issues with the `TReadyToAcceptEvent` event. When writing a message, you can set custom seqNo and creation timestamp, but by default the SDK sets them automatically.

    By default, `Write` runs asynchronously — data from messages is read and stored in an internal buffer, and sending occurs in the background according to the settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. The session automatically reconnects to {{ ydb-short-name }} when connections are broken and retries sending messages as long as possible, according to the setting `RetryPolicy`. Upon receiving an error that makes it impossible to continue, the write session sends `TSessionClosedEvent` with diagnostic information to the user.

    This is how writing multiple messages in an event loop without handlers might look:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write]-[END topic_write]" %}

  - ISimpleBlockingWriteSession

    As a simplified version of `IWriteSession`, `ISimpleBlockingWriteSession` writes through the same internal buffering but without an event loop: it does not use `ContinuationToken` and does not return acknowledgments via `TAcksEvent`. The `Write` method puts a message into the internal buffer; if the buffer is full, the call blocks until space becomes available. The `blockTimeout` parameter limits the wait time. The method returns `true` if the message is accepted into the buffer, and `false` if it fails to write within the allotted time.

    Sending to the server, as with `IWriteSession`, is performed in the background. To wait for all writes to complete and close the session, call `Close()`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_blocking]-[END topic_write_blocking]" %}

  - IProducer

    `TProducerSettings` inherits `TWriteSessionSettings`, so buffering, sending, and reconnection work the same as in `IWriteSession`: `Write` puts a message into the internal buffer, sending to the server happens in the background according to the settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. The producer reconnects to {{ ydb-short-name }} when the connection is broken and retries sending as long as possible, according to `RetryPolicy`. On an unrecoverable error, the producer closes; the status and reason can be obtained from the result of `Write` or `Flush`.

    `Flush` waits for the accumulated data to be delivered to the server; `Close` waits for the remaining messages in the buffer to be sent and terminates the producer.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_producer]-[END topic_write_producer]" %}

    See a detailed example in the [ydb-platform/ydb repository](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_writer/producer/basic_write).

  {% endlist %}
- Go

  To send a message, it is enough to store a Reader in the Data field from which data can be read. You can expect that the data of each message is read once (or until the first error); by the time Write returns, the data will have been read and saved to the internal buffer.

  SeqNo and message creation date are set automatically by default.

  By default, Write is performed asynchronously – data from messages is read and saved to the internal buffer, and sending occurs in the background. The Writer itself reconnects to {{ ydb-short-name }} when the connection is broken and retries sending messages as long as possible. When an error is received after which it is impossible to continue, the Writer stops and subsequent Write calls will fail with an error.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write]-[END topic_write]" %}

  To write by key to multiple partitions, use `WithWriteToManyPartitions(...)` when creating the writer and fill in the `Key` field in `topicwriter.Message`.

  Routing strategies (set in `WithWriterPartitionByKey(...)` or `WithWriterPartitionByPartitionID()`):

  - `BoundPartitionChooser` — the key is matched against the topic partition ranges (`FromBound`/`ToBound`). By default, the key is passed through MurmurHash64 before matching. Recommended for topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning): the SDK updates the boundaries when partitions are split.
  - `KafkaHashPartitionChooser` — analogous to Kafka: MurmurHash of the key modulo the number of partitions. Convenient when migrating from Kafka. Not supported when auto-partitioning is enabled.
  - `WithWriterPartitionByPartitionID` — write to the partition specified in the `PartitionID` field of the message. Does not combine with key-based routing; when a partition is split, the writer must be recreated manually.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_multiwriter]-[END topic_multiwriter]" %}

  See a detailed example with key-based routing, alternative strategies (`KafkaHash` and `PartitionID`), and the transactional variant in the [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicwriter/topicwriter_to_many_partitions.go) repository.
- Python

  To send messages, you can pass either just the message content (bytes, str) or manually set some properties. Objects can be passed one at a time or in an array (list). The `write` method is executed asynchronously. The method returns immediately after the messages are placed in the client's internal buffer, which usually happens quickly. Waiting may occur if the internal buffer is already full and you need to wait until some data is sent to the server.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write]-[END topic_write]" %}

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_manual]-[END topic_write_manual]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write]-[END topic_write]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    The `send` method blocks control until the message is placed in the send queue.
    Placing a message in this queue means that the writer will do everything possible to deliver the message.
    For example, if the write session is interrupted for some reason, the writer will re-establish the connection and try to send this message on a new session.
    However, placing a message in the send queue does not guarantee that the message will eventually be written.
    For example, errors may occur that cause the writer to terminate before the messages from the queue are sent.
    If you need confirmation of successful writing for each message, use an asynchronous writer and check the status returned by the `send` method.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_sync]-[END topic_write_sync]" %}

  - Asynchronous API

    The `send` method in the asynchronous client is non-blocking. It places the message in the send queue.
    The method returns `CompletableFuture<WriteAck>`, which allows you to check whether the message was actually written.
    If the queue is full, a QueueOverflowException will be thrown.
    This is a way to signal to the user that the write stream should be slowed down.
    In this case, you should either skip messages or retry writing with exponential backoff.
    You can also increase the size of the client buffer (`setMaxSendBufferMemorySize`) to handle a larger volume of messages before it fills up.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_async]-[END topic_write_async]" %}

  {% endlist %}
- C#

  Asynchronous writing of a message to a topic.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write]-[END topic_write]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write]-[END topic_write]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_write]-[END topic_write]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages with server storage confirmation

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Responses about message writes on the server arrive at the SDK client as `TAcksEvent` events. A single event may contain responses about several previously sent messages. Response options: write confirmed (`EES_WRITTEN`), write discarded as a duplicate of a previously written message (`EES_ALREADY_WRITTEN`), or write discarded due to a failure (`EES_DISCARDED`).

    Example of setting up a `TAcksEvent` handler for a write session:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

    In such a write session, `TAcksEvent` events will not be delivered to the user via `GetEvent` / `GetEvents`; instead, when receiving confirmations from the server, the SDK will call the passed handler. Similarly, handlers for other event types can be configured.

  - ISimpleBlockingWriteSession

    Unlike `IWriteSession`, `ISimpleBlockingWriteSession` does not return confirmations for individual messages: `TAcksEvent` events and their handlers are not available. To wait until all messages from the buffer are written to the server, call `Close()` — the method waits for confirmation from the server and closes the session.

  - IProducer

    Confirmations from the server arrive the same way as for `IWriteSession`: via the `AcksHandler` handler in `TProducerSettings::EventHandlers`. To wait for the accumulated buffer to be delivered to the server, call `Flush()`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_producer_ack]-[END topic_producer_ack]" %}

  {% endlist %}
- Go

  When connecting, you can specify the synchronous message write option - topicoptions.WithSyncWrite(true). Then Write will return only after receiving confirmation from the server that all messages passed in the call have been saved. At the same time, the SDK will, as usual, reconnect and retry sending messages if necessary. In this mode, the context only controls the timeout for waiting for a response from the SDK, i.e., even after the context is canceled, the SDK will continue trying to send messages.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- Python

  There are two ways to get confirmation that messages have been written to the server:

  - `flush()` — waits for confirmation for all messages previously written to the internal buffer.
  - `write_with_ack(...)` — sends a message and waits for confirmation of its delivery from the server. When sending multiple messages in a row, this method works slowly.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  {% endlist %}
- Java

  The `send` method returns `CompletableFuture<WriteAck>`. Its successful completion means the write is confirmed by the server.
  The `WriteAck` structure contains information about seqNo, offset, and write status:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- C#

  Asynchronous write of a message to a topic. In case of internal buffer overflow, it will wait for the buffer to become free for retransmission.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

  If the server is unavailable, messages may accumulate in a queue waiting to be sent. To control the wait time, you can use a cancellation token (`CancellationToken`). However, with this approach, there is a risk that the user may cancel the sending of an already confirmed message.

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_deadline]-[END topic_write_deadline]" %}

- JavaScript

  All messages are written to an internal buffer. There are 3 mechanisms for sending to the server: two automatic and one manual. The manual one is calling the `writer.flush` method, which returns the last seqno written on the server. Automatic sending occurs under the following conditions:

  - Exceeding the internal buffer size `maxBufferBytes` (default value = 256MiB).
  - By the tick of the periodic send interval `flushIntervalMs` (default value = 10ms).

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_write_ack]-[END topic_write_ack]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Choosing a codec for message compression {#codec}

Learn more about [data compression in topics](../../concepts/datamodel/topic#message-codec).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    The compression used when sending messages via the `Write` method is set when [creating a write session](#start-writer) with the `Codec` and `CompressionLevel` settings. By default, the GZIP codec is selected.
    Example of creating a write session without message compression:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec]-[END topic_codec]" %}

    If you need to send a message compressed with a different codec within a write session, you can use the `WriteEncoded` method specifying the codec and the size of the uncompressed message. For a successful write using this method, the codec used must be allowed in the topic settings.

  - ISimpleBlockingWriteSession

    The codec is set when [creating a write session](#start-writer) in `TWriteSessionSettings` — the same `Codec` and `CompressionLevel` settings as for `IWriteSession`. The `WriteEncoded` method is not available.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec_blocking]-[END topic_codec_blocking]" %}

  - IProducer

    The codec is set in `TProducerSettings` when [creating a producer](#start-writer) — the same `Codec` and `CompressionLevel` settings as for `IWriteSession`.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_codec_producer]-[END topic_codec_producer]" %}

  {% endlist %}
- Go

  By default, the SDK selects the codec automatically (taking into account the topic settings). In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then occasionally tries to compress messages with all available codecs and selects the codec that gives the smallest message size. If the list of allowed codecs for the topic is empty, auto-selection is performed between Raw and Gzip codecs.

  If necessary, you can set a fixed codec in the connection options. Then that codec will be used and no measurements will be performed.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- Python

  By default, the SDK selects the codec automatically (taking into account the topic settings). In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then occasionally tries to compress messages with all available codecs and selects the codec that gives the smallest message size. If the list of allowed codecs for the topic is empty, auto-selection is performed between Raw and Gzip codecs.

  If necessary, you can set a fixed codec in the connection options. Then that codec will be used and no measurements will be performed.

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_codec]-[END topic_codec]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  The JavaScript SDK expects a `CompressionCodec` object with `compress` and `decompress` functions. Import `RAW_CODEC` and `GZIP_CODEC` from `@ydbjs/topic/codec`. For LZOP and custom codecs, provide the implementation; the complete example defines `lzopCodec` and `customCodec`.

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_raw]-[END topic_codec_raw]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_gzip]-[END topic_codec_gzip]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_lzop]-[END topic_codec_lzop]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_codec_custom]-[END topic_codec_custom]" %}

- Rust

  Selecting a compression codec when writing in the Rust SDK is not yet available; messages are sent with the `Raw` codec.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages without deduplication {#nodedup}

For more information about writing without deduplication, see the [corresponding section of concepts](../../concepts/datamodel/topic#no-dedup).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    If the `ProducerId` option is not specified in the write session settings, a write session without deduplication will be created.
    Example of creating such a write session:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_dedup]-[END topic_no_dedup]" %}

    To enable deduplication, you need to specify the `ProducerId` option in the write session settings or explicitly enable deduplication by calling the `DeduplicationEnabled()` method, for example, as in the ["Connecting to a topic"](#start-writer) section.

  - ISimpleBlockingWriteSession

    The behavior is the same as for `IWriteSession`: if `ProducerId` is not specified, the session is created without deduplication.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_dedup_blocking]-[END topic_no_dedup_blocking]" %}

  - IProducer

    `IProducer` always writes with deduplication: the producer ID is formed from `ProducerIdPrefix` and the partition ID. For writing without deduplication, use `IWriteSession` or `ISimpleBlockingWriteSession`.

  {% endlist %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Go

  In **ydb-go-sdk**, when creating a writer, if `topicoptions.WithWriterProducerID` is not passed, the SDK still substitutes the producer ID (generates it automatically). The write mode without deduplication, equivalent to the absence of `ProducerId` in the C++ example above, is not available in the current version of the SDK.
- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing metadata at the message level {#messagemeta}

When writing a message, you can additionally specify metadata as a list of key-value pairs. This data will be available when reading the message.
The metadata size limit is no more than 1000 keys.

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Metadata is set in the `TWriteMessage` object and passed to `Write()`:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  - ISimpleBlockingWriteSession

    The same `TWriteMessage` is used as for `IWriteSession`: metadata is set via `MessageMeta()` and passed to `Write()`:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata_blocking]-[END topic_write_metadata_blocking]" %}

  - IProducer

    As with write sessions, metadata is set in `TWriteMessage` via `MessageMeta()`. The difference of `IProducer` is that the message also contains a partitioning key, by which the producer selects a partition:

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_metadata_producer]-[END topic_write_metadata_producer]" %}

  {% endlist %}
- Go

  Metadata is set in the `Metadata` field of the `topicwriter.Message` structure:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  When reading, metadata is available in the `Metadata` field of the message:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- Java

  When constructing a message for writing using a Builder, you can pass it objects of type `MetadataItem` with a key of type `String` + value of type `byte[]`.

  You can pass `List` such objects at once:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  Or add each `MetadataItem` separately:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_metadata_add]-[END topic_write_metadata_add]" %}

  When reading, you can get these message metadata by calling the `getMetadataItems()` method on it:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- Python

  To use the metadata transfer function, create an `TopicWriterMessage` object with the `metadata_items` argument, as shown below:

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

  {% endlist %}

  During reading, metadata can be obtained from the `metadata_items` field of the `PublicMessage` object:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_metadata]-[END topic_read_metadata]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_write_metadata]-[END topic_write_metadata]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing in a transaction {#write-tx}

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    To write to a topic in a transaction, you need to pass a reference to the transaction object to the `Write` method of the write session.

    [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

    `NextToken` in the complete example waits for `TReadyToAcceptEvent` and returns its `ContinuationToken`, as shown in the writing event loop above. The asynchronous writer requires this token when sending a message in a transaction.

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  - ISimpleBlockingWriteSession

    Like `IWriteSession`, `ISimpleBlockingWriteSession` supports writing in transactions. Since the simple variant does not have `ContinuationToken`, the transaction object is passed as the second argument to `Write()`.

    [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_write_tx_blocking]-[END topic_write_tx_blocking]" %}

  - IProducer

    For `IProducer`, the transaction is specified not by the `Write` argument, but in `TWriteMessage` via `Tx()`. After that, the producer writes the message in the same way as a regular message with a partitioning key:

  {% endlist %}
- Go

  To write to a topic in a transaction, you need to create a transactional writer by calling [TopicClient.StartTransactionalWriter](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic#Client.StartTransactionalWriter). After that, you can send messages as usual. There is no need to close the transactional writer — it happens automatically when the transaction completes.

  [Example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

- Python

  To write to a topic in a transaction, you need to create a transactional writer by calling `topic_client.tx_writer`. After that, you can send messages as usual. There is no need to close the transactional writer — it happens automatically when the transaction completes.

  In the example below, there is no explicit call to `tx.commit()` — it happens implicitly upon successful completion of the `callee` lambda.

  [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  - Native SDK (Asyncio)

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_write_tx]-[END topic_write_tx]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    In the settings of the `SendSettings` method `send`, you can specify a transaction.
    Then the message will be written together with the commit of that transaction.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_tx_sync]-[END topic_write_tx_sync]" %}

  - Asynchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    In the settings of the `SendSettings` method `send`, you can specify a transaction.
    Then the message will be written together with the commit of that transaction.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_write_tx_async]-[END topic_write_tx_async]" %}

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Reading messages {#reading}

### Connecting to a topic for reading messages {#start-reader}

Reading messages from a topic can be performed with or without specifying a Consumer associated with the topic. If a Consumer is not specified, the client application must calculate the offset for reading messages on its own. A more detailed example of reading without a Consumer is provided in the [corresponding section](#no-consumer).

A Consumer can be created when [creating](#create-topic) or [altering](#alter-topic) a topic.
A topic can have multiple Consumers, and the server stores its own read progress for each of them.

{% list tabs group=lang %}

- C++

  A connection for reading from one or more topics is represented by a read session object with the `IReadSession` interface. The read session settings are represented by the `TReadSessionSettings` structure.

  See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Go

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Python

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Initializing reader settings

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_settings]-[END topic_reader_settings]" %}

    Creating a synchronous reader

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader]-[END topic_sync_reader]" %}

    After creating a synchronous reader, you need to initialize it. To do this, use one of two methods:

    - `init()`: non-blocking, starts the initialization process in the background and does not wait for it to complete.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader_background]-[END topic_sync_reader_background]" %}

    - `initAndWait()`: blocking, starts the initialization process and waits for it to complete. If an error occurs during initialization, an exception is thrown.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_sync_reader_init]-[END topic_sync_reader_init]" %}

  - Asynchronous API

    Initializing reader settings

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_settings]-[END topic_reader_settings]" %}

    For an asynchronous reader, in addition to the general read settings `ReaderSettings`, you will need the event handler settings `ReadEventHandlersSettings`, in which you must pass an instance of a `ReadEventHandler` subclass.
    It will describe how to handle various events that occur during reading.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_handler_settings]-[END topic_handler_settings]" %}

    Optionally, in `ReadEventHandlersSettings` you can specify an executor on which message processing will occur; by default, the internal SDK thread is used.

    To implement an event handler, you can inherit from `AbstractReadEventHandler` and override the `onMessages` method.
    The `onMessages` method is called each time the SDK receives the next batch of messages from the server. Within a single call, one or more messages arrive, which can be acknowledged (`commit`) either individually or after processing the entire batch. Example implementation:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch_individual_commit]-[END topic_read_batch_individual_commit]" %}

    Creating and initializing an asynchronous reader:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_async_reader]-[END topic_async_reader]" %}

  {% endlist %}
- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_start_reader]-[END topic_start_reader]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

You can also use the extended connection creation option to specify multiple topics and set read parameters. The following code will create a connection to topics `my-topic` and `my-specific-topic` via the reader `my-consumer`:

{% list tabs group=lang %}

- C++

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

  Also, the example above sets the time from which to start reading messages.
- Python

  Functionality is under development.
- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_partitions]-[END topic_reader_selectors_partitions]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_lag]-[END topic_reader_selectors_lag]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_from]-[END topic_reader_selectors_from]" %}

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_reader_selectors_multiple]-[END topic_reader_selectors_multiple]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_reader_selectors]-[END topic_reader_selectors]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [message read position](../../concepts/datamodel/topic.md#consumer-offset). After reading the next message, the client can [send an acknowledgment of processing to the server](#commit). The read position will change, and on a new connection, only unacknowledged messages will be read.

Messages can also be read [without acknowledgment of processing](#no-commit). In this case, on a new connection, all unacknowledged messages will be read, including those already processed.

Information about which messages have already been processed can be [stored on the client side](#client-commit) by passing the starting read position to the server when creating a connection. In this case, the message read position on the server does not change.

You can use [transactions](#read-tx). In this case, the read position will change when the transaction is committed. On a new connection, all unacknowledged messages will be read.

{% list tabs group=lang %}

- C++

  User interaction with the `IReadSession` object is generally structured as processing an event loop with the following event types: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TEndPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent`, and `TSessionClosedEvent`.

  For each event type, you can set a handler for that event, and you can also set a common handler. Handlers are set in the write session settings before creating it.

  If a handler for a certain event is not set, you must obtain and process it in the `GetEvent` / `GetEvents` methods. For non-blocking waiting for the next event, there is the `WaitEvent` method with the signature `TFuture<void>()`.
- Go

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}
- Python

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}
- Java

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}
- C#

  {% include [_includes/reading_messages_common.md](_includes/reading_messages_common.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Full example of reading a topic in a transaction with writing to a table: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without message processing acknowledgment {#no-commit}

#### Reading messages one by one

{% list tabs group=lang %}

- C++

  Reading messages one by one is not supported in the C++ SDK. The `TDataReceivedEvent` event contains a batch of messages.
- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    To read messages one by one without processing acknowledgment, use the following code:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

  - Asynchronous API

    In the asynchronous client, it is not possible to read messages one by one.

  {% endlist %}
- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_one]-[END topic_read_one]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Reading messages in a batch

{% list tabs group=lang %}

- C++

  When setting up a read session with the `SimpleDataHandlers` setting, it is enough to pass a handler for data messages. The SDK will call this handler for each batch of messages received from the server. Read acknowledgments will not be sent by default.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_session]-[END topic_read_batch_session]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_wait]-[END topic_read_batch_wait]" %}

  In this example, after creating the session, the main thread waits for the session to be closed by the server in the `GetEvent` method; other event types will not arrive.
- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    In the synchronous client, it is not possible to read a batch of messages at once.

  - Asynchronous API

    To read a batch of messages without processing acknowledgment, use the following code:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

  {% endlist %}
- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_batch]-[END topic_read_batch]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with message processing acknowledgment {#commit}

Message processing acknowledgment (commit) tells the server that the message from the topic has been processed by the receiver and no longer needs to be sent. When using reading with acknowledgment, you must acknowledge all received messages without skipping. Message commit on the server occurs after acknowledging the next interval of messages 'without gaps'; the acknowledgments themselves can be sent in any order.

For example, messages 1, 2, 3 arrive from the server. The program processes them in parallel and sends acknowledgments in this order: 1, 3, 2. In this case, message 1 will be committed first, and messages 2 and 3 will be committed only after the server receives acknowledgment of message 2 processing.

If an error occurs on message commit, you can log the error and continue working. The state of the message at that point is unknown. The message might have been committed, and then a network error occurred and the client did not receive the acknowledgment. If the message was not committed, it will be read again and will be processed again (possibly by a different reader). There is no point in retrying the commit itself, because the read session for that message is already lost.

#### Reading messages one by one with acknowledgment

{% list tabs group=lang %}

- C++

  Reading messages one by one is not supported in the C++ SDK. The `TDataReceivedEvent` event contains a batch of messages.
- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  By default, `Commit` is a fast call: it saves data in an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, you need to explicitly close the reader using the `Reader.Close()` call.
- Python

  `commit` is a fast call: it saves data in an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, you need to explicitly close the reader.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  {% endlist %}
- Java

  To confirm message processing, just call the `commit` method on the message.
  This applies to both synchronous and asynchronous readers.
  In an asynchronous reader, when processing a batch of messages, you can call `commit` either on the entire batch at once or on each message individually.
  This method returns `CompletableFuture<Void>`; its successful completion means the server has acknowledged processing.
  If a commit error occurs, do not attempt to retry it. The error is most likely caused by a session closure.
  The reader (not necessarily the same one) will create a new session for this partition, and the message will be read again.

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

- Rust

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_commit]-[END topic_read_commit]" %}

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_commit_ack]-[END topic_read_commit_ack]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Batch reading of messages with acknowledgment

{% list tabs group=lang %}

- C++

  Similarly to [the example above](#no-commit), when setting up a read session with the `SimpleDataHandlers` setting, it is sufficient to pass a handler for data messages. The SDK will call this handler for each message batch received from the server. Passing the `commitDataAfterProcessing = true` parameter means that the SDK will send read acknowledgments for all messages to the server after the handler execution.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit_session]-[END topic_read_batch_commit_session]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_batch_commit_wait]-[END topic_read_batch_commit_wait]" %}

- Go

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  By default, `Commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, the reader must be closed explicitly.
- Python

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% endlist %}

  `commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the latest commits before exiting the program, the reader must be explicitly closed.
- Java

  {% list tabs %}

  - Synchronous API

    Not relevant, because the synchronous reader does not support reading messages in batches.

  - Asynchronous API

    In the `onMessages` handler, you can commit the entire batch of messages by calling `commit` on the event.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

  {% endlist %}
- C#

  {% code "/.generated/sdk-snippets/dotnet/examples/ydb_tech/topic/Program.cs" lang="csharp" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_read_batch_commit]-[END topic_read_batch_commit]" %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with position storage on the client side {#client-commit}

Instead of committing messages to the server, you can store the read progress yourself. In this case, you need to pass a handler to the SDK that will be called when reading of each partition starts. In this handler, you will need to specify the position from which to start reading this partition.

{% list tabs group=lang %}

- C++

  When processing events `TStartPartitionSessionEvent`, you can set the position from which to start reading in the response to the server. To do this, you should pass to the method `Confirm` the parameter `readOffset`. Additionally, you can pass the parameter `commitOffset`, which will specify the position up to which messages should be considered [committed](#commit).

  Example of setting up a handler:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  Here, `GetOffsetToReadFrom` is part of the example, not the SDK. Use your own method to determine the required starting read position for a partition with the given partition id.

  Also, `TReadSessionSettings` supports the `ReadFromTimestamp` setting for reading events with write timestamps not less than a given value. This setting is intended not for precise start positioning, but for skipping a volume of data over a large time interval. The first few received messages may have write timestamps less than the specified one.
- Go

  {% note tip %}

  In the default reader mode, offsets up to the position specified by `res.StartFrom` are committed on the server. After that, re-reading the same messages by moving the position back becomes impossible. To disable automatic acknowledgment, use the no-commit mode when creating the reader.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  {% endnote %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_client_offset_storage]-[END topic_client_offset_storage]" %}

- Python

  The functionality is under development.
- Java

  Reading from a given offset in Java is only possible in the asynchronous reader.
  In the `StartPartitionSessionEvent` event handler, when responding to the server, you can set the position from which to start reading.
  To do this, pass the `StartPartitionSessionSettings` settings with the specified offset via `setReadOffset` to the `confirm` method.
  Also, by calling `setCommitOffset`, you can specify the offset that should be considered committed.

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

  You can also configure the `setReadFrom` reader to read events with write timestamps not less than the given one.
- JavaScript

  {% code "/.generated/sdk-snippets/javascript/examples/ydb_tech/topic/main.ts" lang="typescript" lines="[BEGIN topic_client_offset]-[END topic_client_offset]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without specifying a Consumer {#no-consumer}

Typically, the topic read progress is stored on the server in each `Consumer`. However, you can choose not to store such progress on the server and explicitly specify when creating a reader that reading will occur without `Consumer`.

{% list tabs group=lang %}

- C++

  In `NYdb::NTopic::TReadSessionSettings`, call `WithoutConsumer()`:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

  On reconnection, the read progress is not saved on the server. To avoid starting from the beginning, pass the offset in `TStartPartitionSessionEvent::Confirm` each time a partition reading session starts — see [storing position on the client](#client-commit).
- Go

  You need to pass an empty string as the consumer name and the `topicoptions.WithReaderWithoutConsumer(false)` option (**experimental** mode, see [VERSIONING](https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md) in the SDK repository). In the read selector, specify the topic path and the list of partitions. Message commits are not available in this mode (`CommitModeNone`); on reconnections, progress must be restored on the client side — see [storing position on the client](#client-commit).

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

- Java

  To read without a consumer, you should explicitly specify this in the reader settings `ReaderSettings` by calling `withoutConsumer()`:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

  In this case, note that when the connection is re-established, the progress on the server will be reset. Therefore, to avoid starting reading from the beginning, you should pass the starting read offset in the SDK each time a partition reading session starts:

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_no_consumer_offset]-[END topic_no_consumer_offset]" %}

- Python

  To read without a consumer, create a reader using the `reader` method with the following arguments:

  - `topic` - an `ydb.TopicReaderSelector` object with the specified `path` and list of `partitions`;
  - `consumer` - must be `None`;
  - `event_handler` - a descendant of `ydb.TopicReaderEvents.EventHandler` that implements the `on_partition_get_start_offset` function. This function is responsible for returning the initial offset for reading messages when the reader starts and during reconnections. The client application must specify this offset in the `ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse.start_offset` parameter. The function can also be implemented as asynchronous.

  Example:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_no_consumer_handler]-[END topic_no_consumer_handler]" %}

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_no_consumer]-[END topic_no_consumer]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading in a transaction {#read-tx}

{% list tabs group=lang %}

- C++

  Before reading from a topic, the client code must pass a reference to a transaction object in the session event receiving settings.

  [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/tree/57e19f101afcfa49caab6a8fb24d6c222c362bd2/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  {% note warning %}

  When processing `events` events, you do not need to explicitly acknowledge processing for events of type `TDataReceivedEvent`.

  {% endnote %}

  Acknowledgment of `TStopPartitionSessionEvent` event processing must be done after calling `Commit`.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_read_tx_deferred_stop]-[END topic_read_tx_deferred_stop]" %}

- Go

  To read messages within a transaction, use the [`Reader.PopMessagesBatchTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader#Reader.PopMessagesBatchTx) method. It reads a batch of messages and adds their commit to the transaction, so you do not need to commit these messages separately. The message reader can be reused in different transactions. However, it is important that the order of transaction commits matches the order of messages received from the reader, because message commits in a topic must be performed strictly in order. The easiest way to do this is to use the reader in a loop.

  [Example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/8ad2661ef04180780d314fb0706ec3f8a7ccfc8d/examples/ydb_tech/topic)

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- Python

  To read messages within a transaction, use the `reader.receive_batch_with_tx` method. It reads a batch of messages and adds their commit to the transaction, so you do not need to commit these messages separately. The message reader can be reused in different transactions. However, it is important that the order of transaction commits matches the order of messages received from the reader, because message commits in a topic must be performed strictly in order — otherwise, the transaction will get an error when trying to commit. The easiest way to do this is to use the reader in a loop.

  {% list tabs %}

  - Native SDK

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  - Native SDK (Asyncio)

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/0409ca98c5ee473919298ec09a2c4ec0d5e21a43/examples/ydb_tech/topic)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    In the `ReceiveSettings` settings of the `receive` method, you can specify a transaction:

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_tx_sync]-[END topic_read_tx_sync]" %}

    Then the received message will be committed together with the transaction. You do not need to commit it separately.
    The `receive` method will link the message offsets with the transaction on the server by calling `sendUpdateOffsetsInTransaction` and return control when it receives a response.

  - Asynchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/a220a4a6648b26136315e63349bccc5408e7841b/examples/ydb_tech/topic)

    After receiving a message in the `onMessages` handler, you can associate one or more messages with a transaction.
    To do this, call a separate method `reader.updateOffsetsInTransaction` and wait for its execution on the server.
    This method takes a list of offsets as a parameter. For convenience, `Message` and `DataReceivedEvent` have a method `getPartitionOffsets()` that returns such a list.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_read_tx_async]-[END topic_read_tx_async]" %}

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Full example of reading a topic in a transaction with writing to a table: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).

  {% code "/.generated/sdk-snippets/rust/ydb/examples/ydb_tech/topic/main.rs" lang="rust" lines="[BEGIN topic_read_tx]-[END topic_read_tx]" %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Handling server read interruption {#stop}

In {{ ydb-short-name }}, server-side balancing of partitions between clients is used. This means that the server can interrupt reading messages from arbitrary partitions.

With a *soft interrupt*, the client receives a notification that the server has finished sending messages from the partition and no more messages will be read. The client can finish processing the messages and send an acknowledgment to the server.

In case of a *hard interrupt*, the client receives a notification that it can no longer work with partition messages. The client must stop processing the read messages. Unacknowledged messages will be passed to another reader.

#### Soft interrupt {#soft-stop}

{% list tabs group=lang %}

- C++

  A soft interrupt arrives as an event `TStopPartitionSessionEvent` with method `Confirm`. The client can finish processing messages and send an acknowledgment to the server.

  A fragment of the event loop might look like this:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

- Go

  The client code immediately receives all messages available in the buffer (on the SDK side), even if they are not enough to form a packet during batch processing.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

- Python

  No special processing is required.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Not relevant, as the synchronous reader does not allow configuring handling of such events.
    The client immediately responds to the server with a stop acknowledgment.

  - Asynchronous API

    To be able to respond to such an event, you should override the `onStopPartitionSession(StopPartitionSessionEvent event)` method in the `ReadEventHandler` descendant object (see [Connecting to a topic for reading messages](#start-reader)).
    `event.confirm()` must be called, because the server expects this response to continue the shutdown.

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_soft_stop]-[END topic_soft_stop]" %}

  {% endlist %}
- C#

  No special processing is required.
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Rust SDK handles stop and close events of partition session internally; there is no public API for configuring soft or hard interrupt yet.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Hard interrupt {#hard-stop}

{% list tabs group=lang %}

- C++

  A hard interrupt comes as an `TPartitionSessionClosedEvent` event either in response to acknowledgment of a soft interrupt, or when the connection to the partition is lost. You can find out the reason by calling the `GetReason` method.

  An event loop fragment may look like this:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

- Go

  When reading is interrupted, the context of the message or batch of messages is cancelled.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

- Python

  In this example, message processing in a batch will stop if the partition is reassigned during operation. This optimization requires additional code on the client side. In simple cases, when processing reassigned partitions is not a problem, it can be omitted.

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Not relevant, because the synchronous reader does not provide the ability to configure handling of such events.

  - Asynchronous API

    {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_hard_stop]-[END topic_hard_stop]" %}

  {% endlist %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Rust SDK handles stop and close events of a partition session internally; there is no public API for configuring soft or hard interrupt yet.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Auto-scaling support for topics {#autoscaling}

{% note warning %}

`min_active_partitions` and `max_active_partitions` do not enable topic
autoscaling by themselves. If `auto_partitioning_settings` is omitted, the
strategy remains `DISABLED` and the topic has a fixed number of active
partitions. In YDB 26.2, `describe_topic()` then reports the effective
`max_active_partitions` as equal to `min_active_partitions`.

To configure different minimum and maximum limits, also set the strategy to
`TopicAutoPartitioningStrategy.SCALE_UP`, as shown below.

{% endnote %}

{% list tabs group=lang %}

- C++

  The SDK supports two modes for reading topics with auto-scaling enabled: full support mode and compatibility mode. The reading mode is set in the read session creation parameters. By default, compatibility mode is used.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

  In full support mode, when all messages from a partition have been read, the `TEndPartitionSessionEvent` event arrives. After receiving this event, no new messages will appear in the partition for reading. To continue reading from child partitions, you must call `Confirm()`, thereby confirming that the application is ready to receive messages from child partitions. If messages from all partitions are processed in a single thread, then `Confirm()` can be called immediately after receiving `TEndPartitionSessionEvent`. If messages from different partitions are processed in different threads, you should finish processing the messages, for example, execute the accumulated batch, commit them, or save the read position in your own database, and only then call `Confirm()`.

  After receiving `TEndPartitionSessionEvent` and processing all messages, it is recommended to always immediately commit them. This will balance the reading of child partitions among different read sessions, leading to an even distribution of load across all readers.

  A fragment of the event loop might look like this:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_autoscale_end]-[END topic_autoscale_end]" %}

  In compatibility mode, there is no explicit signal that reading from a partition is complete, and the server will try to heuristically determine that the client has processed the partition to the end. This may cause a delay between finishing reading from the original partition and starting reading from its child partitions.

  If the client commits messages, then the signal that processing of messages from a partition is complete will be the commit of the last message of that partition. If the client does not commit messages, the server will periodically interrupt reading from the partition and switch to reading in another session (if there are other sessions ready to process the partition). This will continue until reading [starts](#client-commit) from the end of the partition.

  It is recommended to check the correctness of handling a soft read interrupt: the client must process the received messages, commit them or save the read position in its own database, and only then call `Confirm()` for the `TStopPartitionSessionEvent` event.
- Go

  Enabling topic autoscaling during its creation is done using the `topicoptions.CreateWithAutoPartitioningSettings` option:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_create_basic]-[END topic_autoscale_create_basic]" %}

  If necessary, you can also set other parameters in AutoPartitioningSettings:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  Enabling autoscaling for an existing topic is done using the `topicoptions.AlterWithAutoPartitioningStrategy` option of `.Topic().Alter`:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_alter_basic]-[END topic_autoscale_alter_basic]" %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_alter]-[END topic_autoscale_alter]" %}

  The SDK supports two modes for reading topics with autoscaling enabled: full support mode and compatibility mode. The reading mode is set by the `topicoptions.WithReaderSupportSplitMergePartitions` option when creating the reader. By default, full support mode (`true`) is used.

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

- Python

  Enabling topic autoscaling during its creation is done using the `auto_partitioning_settings` argument of `create_topic`:

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_autoscale_create]-[END topic_autoscale_create]" %}

  {% endlist %}

  Modifying an existing topic is done using the `alter_auto_partitioning_settings` argument of `alter_topic`:

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_alter]-[END topic_autoscale_alter]" %}

  The SDK supports two modes for reading topics with autoscaling enabled: full support mode and compatibility mode. The reading mode is set by the `auto_partitioning_support` argument when creating the reader. By default, full support mode is used.

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_reader_full]-[END topic_autoscale_reader_full]" %}

  {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_autoscale_reader_compat]-[END topic_autoscale_reader_compat]" %}

  From a practical point of view, the modes do not differ for the end user. The full support mode differs from the compatibility mode in who guarantees the reading order — the client or the server. The compatibility mode is achieved by server-side processing and is generally slower.
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#311](https://github.com/ydb-platform/ydb-rs-sdk/issues/311)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Commit Outside the Reader {#commit-outside-the-reader}

Most often, it is convenient to commit within the reader that receives messages. However, there are scenarios where the commit must be performed by a process different from the reading process. In such a case, a commit method outside the reader is needed.

{% list tabs group=lang %}

- C++

  Committing outside the read session is done using the `NYdb::NTopic::TTopicClient::CommitOffset` method:

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  If there is an active read session at the time of commit (for example, via `CreateReadSession`), it is recommended to pass its ID using the `ReadSessionId` option in `NYdb::NTopic::TCommitOffsetSettings`. This allows the server not to interrupt the current read session:

  In this SDK version, `IReadSession::GetSessionId()` identifies the local SDK object. Obtain the active server session ID from the partition consumer statistics returned by `DescribeConsumer`.

  {% code "/.generated/sdk-snippets/cpp/examples/ydb_tech/topic/main.cpp" lang="cpp" lines="[BEGIN topic_commit_outside_session]-[END topic_commit_outside_session]" %}

- Go

  Acknowledgment of processing outside the reader is performed using the `db.Topic().CommitOffset` method:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  If there is an active read session at the time of commit (via `StartReader` or `StartListener`), it is recommended to pass its ID using the `WithCommitOffsetReadSessionID` option. This allows the server not to interrupt the current read session:

  {% code "/.generated/sdk-snippets/go/examples/ydb_tech/topic/main.go" lang="go" lines="[BEGIN topic_commit_outside_session]-[END topic_commit_outside_session]" %}

- Python

  Acknowledgment of processing outside the reader is performed using the `topic_client.commit_offset` method:

  {% list tabs %}

  - Native SDK

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/sync_example.py" lang="python" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  - Native SDK (Asyncio)

    {% code "/.generated/sdk-snippets/python/examples/ydb_tech/topic/async_example.py" lang="python" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

  {% endlist %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java

  {% code "/.generated/sdk-snippets/java/examples/ydb_tech/topic/src/main/java/tech/ydb/examples/topic/TopicExample.java" lang="java" lines="[BEGIN topic_commit_outside]-[END topic_commit_outside]" %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
