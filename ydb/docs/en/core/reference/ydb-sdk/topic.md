# Working with topics

This article provides examples of using the {{ ydb-short-name }} SDK to work with [topics](../../concepts/datamodel/topic.md).

Before running the examples, [create a topic](../ydb-cli/topic-create.md) and [add a reader](../ydb-cli/topic-consumer-add.md).

## Examples of working with topics

{% list tabs group=lang %}

- C++

  [Reader example on GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_reader)
- Go

  [Examples on GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples/topic)
- Java

  [Examples on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/master/ydb-cookbook/src/main/java/tech/ydb/examples/topic)
- Python

  [Examples on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/topic)
- C#

  [Examples on GitHub](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/src/Topic)
- JavaScript

  [Examples on GitHub](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples/topic)
- Rust

  [Examples on GitHub](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples) (`topic-writer`, `topic-reader-retry`, `topic-read-in-transaction-example`).
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Initializing a connection to topics {#init}

{% list tabs group=lang %}

- C++

  To work with topics, instances of the {{ ydb-short-name }} driver and client are created.

  The {{ ydb-short-name }} driver is responsible for the interaction between the application and {{ ydb-short-name }} at the transport level. The driver must exist throughout the entire lifecycle of working with topics and must be initialized before creating the client.

  The topic service client ( [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) runs on top of the {{ ydb-short-name }} driver and is responsible for management operations on topics, as well as creating read and write sessions.

  Application code snippet for initializing the {{ ydb-short-name }} driver:


  ```cpp
  // Create driver instance.
  auto driverConfig = NYdb::TDriverConfig()
      .SetEndpoint(opts.Endpoint)
      .SetDatabase(opts.Database)
      .SetAuthToken(std::getenv("YDB_TOKEN"));

  NYdb::TDriver driver(driverConfig);
  ```


  This example uses an authentication token stored in the `YDB_TOKEN` environment variable. For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  Application code snippet for creating a client:


  ```cpp
  NYdb::NTopic::TTopicClient topicClient(driver);
  ```

- Go

  To work with topics, an instance of the {{ ydb-short-name }} driver created using `ydb.Open` is used. The topic client is available via the `db.Topic()` method.


  ```go
  package main

  import (
    "context"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)

    // db.Topic() — client for working with topics
    writer, err := db.Topic().StartWriter("topic-path")
    if err != nil {
      panic(err)
    }

    reader, err := db.Topic().StartReader("consumer-name",
      topicoptions.ReadTopic("topic-path"),
    )
    if err != nil {
      panic(err)
    }
    _ = writer
    _ = reader
  }
  ```

- Java

  To work with topics, instances of the {{ ydb-short-name }} transport and client are created.

  The {{ ydb-short-name }} transport is responsible for the interaction between the application and {{ ydb-short-name }} at the transport level. It must exist throughout the entire lifecycle of working with topics and must be initialized before creating the client.

  Application code snippet for initializing the {{ ydb-short-name }} transport:


  ```java
  try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
          .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
          .build()) {
      // Use YDB transport
  }
  ```


  This example uses the helper method `CloudAuthHelper.getAuthProviderFromEnviron()`, which obtains a token from environment variables.
  For example, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  The topic service client ( [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) runs on top of the {{ ydb-short-name }} transport and is responsible for both management operations on topics and creating writers and readers.

  Application code snippet for creating a client:


  ```java
  try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {
    // Use topic client
  }
  ```


  Both code examples above use a ( [try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)) block.
  This allows automatically closing the client and transport when exiting this block, as both are descendants of `AutoCloseable`.
- C#

  To work with topics, simply pass the connection string directly to the constructor of the required client.

  This example uses anonymous authentication. For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

  Application code snippet for creating various topic clients:


  ```c#
  const string connectionString = "Host=localhost;Port=2136;Database=/local";

  await using var topicClient = new TopicClient(connectionString);

  await using var writer = new WriterBuilder<string>(connectionString, topicName)
  {
      ProducerId = "ProducerId_Example"
  }.Build();

  await using var reader = new ReaderBuilder<string>(connectionString)
  {
      ConsumerName = "Consumer_Example",
      SubscribeSettings = { new SubscribeSettings(topicName) }
  }.Build();
  ```

- Python

  To work with topics, an instance of the {{ ydb-short-name }} driver is created. The topic client is available via the `topic_client` attribute and is used for management operations on topics, as well as creating writers and readers.

  {% list tabs %}

  - Native SDK

    ```python
    import os
    import ydb

    driver_config = ydb.DriverConfig(
        endpoint=os.environ["YDB_ENDPOINT"],
        database=os.environ["YDB_DATABASE"],
    )
    driver = ydb.Driver(driver_config)
    driver.wait(timeout=5)
    # driver.topic_client — client for working with topics
    writer = driver.topic_client.writer(topic_path)
    reader = driver.topic_client.reader(topic=topic_path, consumer=consumer_name)
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb

    driver_config = ydb.DriverConfig(
        endpoint=os.environ["YDB_ENDPOINT"],
        database=os.environ["YDB_DATABASE"],
    )
    async with ydb.aio.Driver(driver_config) as driver:
        await driver.wait(timeout=5)
        # driver.topic_client — client for working with topics
        writer = driver.topic_client.writer(topic_path)
        reader = driver.topic_client.reader(topic=topic_path, consumer=consumer_name)
    ```

  {% endlist %}

  For more information, see [connecting to a database](../../concepts/connect.md) and [authentication](../../security/authentication.md).
- JavaScript


  ```javascript
  const t = topic(driver);

  await using reader = t.createReader({
    topic: "/Root/demo-topic",
    consumer: "demo-consumer",
  });

  await using writer = t.createWriter({
    topic: "/Root/demo-topic",
    producer: "demo-producer",
  });
  ```

- Rust


  ```rust
  use ydb::{ClientBuilder, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
      client.wait().await?;
      let mut topic_client = client.topic_client();
      // topic_client.create_reader(...), create_writer_with_params(...), ...
      Ok(())
  }
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Topic management {#manage}

### Creating a topic {#create-topic}

The only required parameter for creating a topic is its path; the other parameters are optional.

{% list tabs group=lang %}

- C++

  The full list of settings can be found [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

  Example of creating a topic with three partitions and ZSTD codec support:


  ```cpp
  auto settings = NYdb::NTopic::TCreateTopicSettings()
      .PartitioningSettings(3, 3)
      .AppendSupportedCodecs(NYdb::NTopic::ECodec::ZSTD);

  auto status = topicClient
      .CreateTopic("my-topic", settings)  // returns TFuture<TStatus>
      .GetValueSync();
  ```

- Go

  The full list of supported parameters can be found in the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions


  ```go
  err := db.Topic().Create(ctx, "topic-path",
    // optional
    topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

    // optional
    topicoptions.CreateWithMinActivePartitions(3),
  )
  ```

- Python

  Example of creating a topic with a list of supported codecs and a minimum number of partitions

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.create_topic(topic_path,
        supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
        min_active_partitions=3,                                    # optional
    )
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.create_topic(topic_path,
        supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP],  # optional
        min_active_partitions=3,                                     # optional
    )
    ```

  {% endlist %}
- Java

  The full list of settings can be found [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions


  ```java
  topicClient.createTopic(topicPath, CreateTopicSettings.newBuilder()
                  // Optional
                  .setSupportedCodecs(SupportedCodecs.newBuilder()
                          .addCodec(Codec.RAW)
                          .addCodec(Codec.GZIP)
                          .build())
                  // Optional
                  .setPartitioningSettings(PartitioningSettings.newBuilder()
                          .setMinActivePartitions(3)
                          .build())
                  .build());
  ```

- C#

  Example of creating a topic with a list of supported codecs and a minimum number of partitions:


  ```c#
  await topicClient.CreateTopic(new CreateTopicSettings
  {
      Path = topicName,
      Consumers = { new Consumer("Consumer_Example") },
      SupportedCodecs = { Codec.Raw, Codec.Gzip },
      PartitioningSettings = new PartitioningSettings
      {
          MinActivePartitions = 3
      }
  });
  ```

- JavaScript


  ```javascript
  const topicService = driver.createClient(TopicServiceDefinition);
  await topicService.createTopic(
    create(CreateTopicRequestSchema, {
      path: "/path-to-my-topic",
      partitioningSettings: {
        minActivePartitions: 1n,
        maxActivePartitions: 100n,
      },
      consumers: [{ name: "my-consumer" }],
    }),
  );
  ```

- Rust


  ```rust
  use ydb::{Codec, CreateTopicOptionsBuilder, YdbResult};

  topic_client
      .create_topic(
          "/local/my-topic".into(),
          CreateTopicOptionsBuilder::default()
              .min_active_partitions(3)
              .supported_codecs(vec![Codec::Raw, Codec::Zstd])
              .build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Modifying a topic {#alter-topic}

{% list tabs group=lang %}

- C++

  When modifying a topic, specify the topic path and the parameters to be changed in the `AlterTopic` method parameters. The parameters to be changed are represented by the `TAlterTopicSettings` structure.

  You can view the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  An example of adding an [important reader](../../concepts/datamodel/topic#important-consumer) to a topic and setting the [message retention time](../../concepts/datamodel/topic#retention-time) for the topic to two days:


  ```cpp
  auto alterSettings = NYdb::NTopic::TAlterTopicSettings()
      .BeginAddConsumer("my-consumer")
          .Important(true)
      .EndAddConsumer()
      .SetRetentionPeriod(TDuration::Days(2));

  auto status = topicClient
      .AlterTopic("my-topic", alterSettings)  // returns TFuture<TStatus>
      .GetValueSync();
  ```

- Go

  When modifying a topic, specify the topic path and the parameters to be changed in the parameters.

  You can view the full list of supported parameters in the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

  An example of adding a reader to a topic


  ```go
  err := db.Topic().Alter(ctx, "topic-path",
    topicoptions.AlterWithAddConsumers(topictypes.Consumer{
      Name:            "new-consumer",
      SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
    }),
  )
  ```

- Python

  An example of changing the list of supported codecs and the minimum number of partitions for a topic

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.alter_topic(topic_path,
        set_supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
        set_min_active_partitions=3,                                    # optional
    )
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.alter_topic(topic_path,
        set_supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP],  # optional
        set_min_active_partitions=3,                                     # optional
    )
    ```

  {% endlist %}
- Java

  When modifying a topic, specify the topic path and the parameters to be changed in the `alterTopic` method parameters.

  You can view the full list of settings [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/AlterTopicSettings.java#L23).


  ```java
  topicClient.alterTopic(topicPath, AlterTopicSettings.newBuilder()
                  .addAddConsumer(Consumer.newBuilder()
                          .setName("new-consumer")
                          .setSupportedCodecs(SupportedCodecs.newBuilder()
                                  .addCodec(Codec.RAW)
                                  .addCodec(Codec.GZIP)
                                  .build())
                          .build())
                  .build());
  ```

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript


  ```javascript
  const topicService = driver.createClient(TopicServiceDefinition);
  await topicService.alterTopic(
    create(AlterTopicRequestSchema, {
      path: "/path-to-my-topic",
      addConsumers: [{ name: "my-consumer-2" }],
    }),
  );
  ```

- Rust


  ```rust
  use ydb::{AlterTopicOptionsBuilder, YdbResult};

  topic_client
      .alter_topic(
          "/local/my-topic".into(),
          AlterTopicOptionsBuilder::default()
              .set_min_active_partitions(Some(5))
              .build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Getting information about a topic {#describe-topic}

{% list tabs group=lang %}

- C++

  The `DescribeTopic` method is used to get information about a topic.

  The topic description is represented by the `TTopicDescription` structure.

  See the full list of description fields [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L163).

  You can access this description as follows:


  ```cpp
  auto result = topicClient.DescribeTopic("my-topic").GetValueSync();
  if (result.IsSuccess()) {
      const auto& description = result.GetTopicDescription();
      std::cout << "Topic description: " << GetProto(description) << std::endl;
  }
  ```


  There is a separate method for getting information about a reader - `DescribeConsumer`.
- Go


  ```go
    descResult, err := db.Topic().Describe(ctx, "topic-path")
  if err != nil {
    log.Fatalf("failed describe topic: %v", err)
    return
  }
  fmt.Printf("describe: %#v\n", descResult)
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    info = driver.topic_client.describe_topic(topic_path)
    print(info)
    ```

  - Native SDK (Asyncio)

    ```python
    info = await driver.topic_client.describe_topic(topic_path)
    print(info)
    ```

  {% endlist %}
- Java

  The `describeTopic` method is used to get information about a topic.

  You can view the full list of description fields [in the SDK code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).


  ```java
  Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
          .join();
  TopicDescription description = topicDescriptionResult.getValue();
  ```

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript


  ```javascript
  const topicService = driver.createClient(TopicServiceDefinition);
  await topicService.describeTopic(
    create(DescribeTopicRequestSchema, {
      path: "/path-to-my-topic",
    }),
  );
  ```

- Rust


  ```rust
  use ydb::{DescribeTopicOptionsBuilder, YdbResult};

  let description = topic_client
      .describe_topic(
          "/local/my-topic".into(),
          DescribeTopicOptionsBuilder::default().include_stats(true).build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Deleting a topic {#drop-topic}

To delete a topic, just specify its path.

{% list tabs group=lang %}

- C++


  ```cpp
  auto status = topicClient.DropTopic("my-topic").GetValueSync();
  ```

- Go


  ```go
    err := db.Topic().Drop(ctx, "topic-path")
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.drop_topic(topic_path)
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.drop_topic(topic_path)
    ```

  {% endlist %}
- Java


  ```java
  topicClient.dropTopic(topicPath);
  ```

- C#


  ```c#
  await topicClient.DropTopic(topicName);
  ```

- JavaScript


  ```javascript
  const topicService = driver.createClient(TopicServiceDefinition);
  await topicService.dropTopic(
    create(DropTopicRequestSchema, {
      path: "/path-to-my-topic",
    }),
  );
  ```

- Rust


  ```rust
  topic_client.drop_topic("/local/my-topic".into()).await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Writing messages {#write}

### Connecting to a topic for writing messages {#start-writer}

Currently, only connections with matching [source and message group](../../concepts/datamodel/topic#producer-id) identifiers (`producer_id` and `message_group_id`) are supported; this limitation will be removed in the future.

{% list tabs group=lang %}

- C++

  The C++ SDK provides three API options for writing to a topic. The basic settings (buffering, codecs, retries) are the same for all three and are set by the `TWriteSessionSettings` structure, so the tips below focus on the differences and use cases.

  - `IWriteSession` — a low-level write session to a single partition with a full set of features: event loop (`TReadyToAcceptEvent`, `TAcksEvent`, `TSessionClosedEvent`), explicit back-pressure via `TContinuationToken`, per-message acknowledgments, and sending pre-compressed data via `WriteEncoded`. The `TWriteSessionSettings` structure is defined here — the other two options reuse it. Suitable when you need the status of each message, custom asynchronous logic, or manual compression control.
  - `ISimpleBlockingWriteSession` — a synchronous fire-and-forget API for writing to a single topic partition, the simplest option. The `Write(message, blockTimeout)` method puts a message into an internal buffer; sending to the server happens in the background. In normal mode, the call returns immediately and blocks only when the buffer is full (by `MaxMemoryUsage` / `MaxInflightCount`), for no longer than `blockTimeout`. A return of `false` means the message **did not enter the buffer** and is lost. There are no per-message acknowledgments; you can only verify that the entire buffer has been delivered to the server by calling `Close()` — it waits for an ack from the server. Suitable when an "all or nothing" guarantee by session close is sufficient and you need simple synchronous code.
  - `IProducer` — a high-level API over multiple write sessions: it transparently shards messages across topic partitions by key. Inspired by the Apache Kafka Producer interface, but takes into account the specifics of {{ ydb-short-name }} and, when working with topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning), provides full ordering and exactly-once guarantees. Server acknowledgments are available via the `AcksHandler` handler; to wait for the accumulated buffer to be delivered — `Flush()`, to shut down correctly — `Close()`. Suitable when you need to write to a **multi-partition** topic with key-based routing.

  {% list tabs %}

  - IWriteSession

    `IWriteSession` — a basic write session from which other write options inherit settings. The write session settings are represented by the `TWriteSessionSettings` structure; for the `ISimpleBlockingWriteSession` option, some settings are not supported.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).


    ```cpp
    std::string producerAndGroupID = "group-id";
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path("my-topic")
        .ProducerId(producerAndGroupID)
        .MessageGroupId(producerAndGroupID);

    auto session = topicClient.CreateWriteSession(settings);
    ```

  - ISimpleBlockingWriteSession

    `ISimpleBlockingWriteSession` is a simple synchronous write session `IWriteSession` for writing one message at a time without acknowledgment for each message. The `Write` method blocks when the number of inflight records or the SDK buffer size is exceeded. Write session settings are represented by the `TWriteSessionSettings` structure, as in the case of `IWriteSession`, but some settings are not supported.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h#L56).


    ```cpp
    std::string producerAndGroupID = "group-id";
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path("my-topic")
        .ProducerId(producerAndGroupID)
        .MessageGroupId(producerAndGroupID);

    auto session = topicClient.CreateSimpleBlockingWriteSession(settings);
    ```

  - IProducer

    `IProducer` is a high-level API on top of write sessions: a single object hides the management of multiple sessions and automatically selects a partition based on the message key. Producer settings are represented by the `TProducerSettings` structure, which inherits `TWriteSessionSettings`, so the common write settings match `IWriteSession`.

    Settings are set via `TProducerSettings`:

    - `ProducerIdPrefix` — producer ID prefix for write subsessions;
    - `PartitionChooserStrategy` — partition selection strategy based on the message key:

      - `Bound` — the key is mapped to topic partition ranges (`FromBound`/`ToBound` from the topic description). By default, the key is passed through MurmurHash64 before mapping. Recommended for topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning): when a partition splits, the SDK updates the boundaries and continues to route messages with the same key to the correct range.
      - `KafkaHash` — by analogy with Kafka: MurmurHash is computed from the key, and the partition index is the remainder of dividing the hash by the number of partitions. Convenient when migrating from Kafka. Not supported when auto-partitioning is enabled.
    - `PartitioningKeyHasher` — key transformation function before mapping to ranges; used only for the `Bound` strategy. You can set your own, for example, so that the original key without hashing participates in the comparison.

    See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/producer.h#L10).


    ```cpp
    auto producerSettings = NYdb::NTopic::TProducerSettings()
        .Path("my-topic")
        .ProducerIdPrefix("my-producer")
        .PartitionChooserStrategy(NYdb::NTopic::EPartitionChooserStrategy::Bound);

    auto producer = topicClient.CreateProducer(producerSettings);
    ```

  {% endlist %}
- Go


  ```go
  producerAndGroupID := "group-id"
  writer, err := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
  )
  if err != nil {
      return err
  }
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    writer = driver.topic_client.writer(topic_path)
    ```

  - Native SDK (Asyncio)

    ```python
    writer = driver.topic_client.writer(topic_path)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Initializing writer settings:


    ```java
    String producerAndGroupID = "group-id";
    WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .build();
    ```


    Creating a synchronous writer:


    ```java
    SyncWriter writer = topicClient.createSyncWriter(settings);
    ```


    After creating the writer, you need to initialize it. There are two methods for this:

    - `init()`: non-blocking, starts the initialization process in the background and does not wait for it to complete.


      ```java
      writer.init();
      ```

    - `initAndWait()`: blocking, starts the initialization process and waits for it to complete. If an error occurs during initialization, an exception is thrown.


      ```java
      try {
          writer.initAndWait();
          logger.info("Init finished successfully");
      } catch (Exception exception) {
          logger.error("Exception while initializing writer: ", exception);
          return;
      }
      ```

  - Asynchronous API

    Initializing writer settings:


    ```java
    String producerAndGroupID = "group-id";
    WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .build();
    ```


    Creating and initializing an asynchronous writer:


    ```java
    AsyncWriter writer = topicClient.createAsyncWriter(settings);

    // Init in background
    writer.init()
            .thenRun(() -> logger.info("Init finished successfully"))
            .exceptionally(ex -> {
                logger.error("Init failed with ex: ", ex);
                return null;
            });
    ```

  {% endlist %}
- C#


  ```c#
  await using var writer = new WriterBuilder<string>(connectionString, topicName)
  {
      ProducerId = "ProducerId_Example"
  }.Build();
  ```

- JavaScript


  ```javascript
  await using writer = createTopicWriter(driver, {
    topic: topicName,
    producer: producerName,
  });
  ```

- Rust


  ```rust
  use ydb::{TopicWriter, TopicWriterOptionsBuilder, YdbResult};

  let writer: TopicWriter = topic_client
      .create_writer_with_params(
          TopicWriterOptionsBuilder::default()
              .topic_path("/local/my-topic".into())
              .producer_id("group-id".into())
              .message_group_id("group-id".into())
              .build()?,
      )
      .await?;
  ```

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

    If a handler for a certain event is not set, you need to get and process it in the `GetEvent` / `GetEvents` methods. For non-blocking waiting for the next event, there is the `WaitEvent` method with the `TFuture<void>()` interface.

    To write each message, the user must "spend" a move-only object `TContinuationToken` that the SDK provides with the `TReadyToAcceptEvent` event. When writing a message, you can set custom seqNo and creation timestamp, but by default the SDK sets them automatically.

    By default, `Write` is executed asynchronously: data from messages is read and stored in an internal buffer, and sending occurs in the background according to the settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. The session itself reconnects to {{ ydb-short-name }} when the connection is broken and retries sending messages as long as possible, according to the setting `RetryPolicy`. When an error is received after which it is impossible to continue, the write session sends `TSessionClosedEvent` with diagnostic information to the user.

    This is how writing multiple messages in an event loop without using handlers might look:


    ```cpp
    // Event loop
    while (true) {
        // Get event
        // May block for a while if write session is busy
        std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);

        if (auto* readyEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
            session->Write(std::move(event.ContinuationToken), "This is yet another message.");

        } else if (auto* ackEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*event)) {
            std::cout << ackEvent->DebugString() << std::endl;

        } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
            break;
        }
    }
    ```

  - ISimpleBlockingWriteSession

    As a simplified version of `IWriteSession`, `ISimpleBlockingWriteSession` writes through the same internal buffering, but without an event loop: it does not use `ContinuationToken` and does not return acknowledgments via `TAcksEvent`. The `Write` method puts a message into the internal buffer; if the buffer is full, the call blocks until space becomes available. The `blockTimeout` parameter limits the waiting time. The method returns `true` if the message is accepted into the buffer, and `false` if it fails to write within the allotted time.

    Sending to the server, like `IWriteSession`, is performed in the background. To wait for all writes to complete and close the session, call `Close()`.


    ```cpp
    auto messageData = std::string("message");
    NYdb::NTopic::TWriteMessage writeMessage(messageData);
    session->Write(std::move(writeMessage));
    ```

  - IProducer

    `TProducerSettings` inherits from `TWriteSessionSettings`, so buffering, sending, and reconnection work the same as in `IWriteSession`: `Write` puts a message into an internal buffer, sending to the server happens in the background according to the settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, `BatchFlushSizeBytes`. The producer reconnects to {{ ydb-short-name }} when connections are broken and retries sending as long as possible, in accordance with `RetryPolicy`. On a fatal error, the producer closes; the status and reason can be obtained from the result of `Write` or `Flush`.

    `Flush` waits for delivery of accumulated data to the server; `Close` waits for sending the remaining messages in the buffer and terminates the producer.


    ```cpp
    auto messageData = std::string("order-created");
    // First argument is the partitioning key — the SDK chooses a partition by it.
    NYdb::NTopic::TWriteMessage writeMessage("user-42", messageData);
    producer->Write(std::move(writeMessage));
    producer->Flush().GetValueSync();
    ```


    See a detailed example in the [ydb-platform/ydb repository](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_writer/producer/basic_write).

  {% endlist %}
- Go

  To send a message, it is enough to store a Reader in the Data field from which data can be read. You can expect that the data of each message is read once (or until the first error), and by the time Write returns, the data will have been read and saved to the internal buffer.

  SeqNo and message creation date are set automatically by default.

  By default, Write is performed asynchronously - data from messages is read and saved to the internal buffer, and sending occurs in the background. The Writer itself reconnects to {{ ydb-short-name }} on connection breaks and retries sending messages as long as possible. Upon receiving an error after which it is impossible to continue, the Writer stops and subsequent Write calls will fail with an error.


  ```go
  err := writer.Write(ctx,
    topicwriter.Message{Data: strings.NewReader("1")},
    topicwriter.Message{Data: bytes.NewReader([]byte{1,2,3})},
    topicwriter.Message{Data: strings.NewReader("3")},
  )
  if err != nil {
    return err
  }
  ```


  To write by key to multiple partitions, use `WithWriteToManyPartitions(...)` when creating the writer and fill in the `Key` field in `topicwriter.Message`.

  Routing strategies (set in `WithWriterPartitionByKey(...)` or `WithWriterPartitionByPartitionID()`):

  - `BoundPartitionChooser` — the key is matched against the topic partition ranges (`FromBound`/`ToBound`). By default, before matching, the key passes through MurmurHash64. Recommended for topics with [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning): the SDK updates the boundaries when partitions split.
  - `KafkaHashPartitionChooser` — analogous to Kafka: MurmurHash of the key modulo the number of partitions. Convenient when migrating from Kafka. Not supported when auto-partitioning is enabled.
  - `WithWriterPartitionByPartitionID` — write to the partition specified in the `PartitionID` field of the message. Does not combine with key routing; when a partition splits, the writer must be recreated manually.


  ```go
  writer, err := db.Topic().StartWriter(topicPath,
    topicoptions.WithWriteToManyPartitions(
      topicoptions.WithProducerIDPrefix("orders-producer"),
      topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
    ),
  )
  if err != nil {
    return err
  }
  defer func() { _ = writer.Close(context.Background()) }()

  err = writer.Write(ctx, topicwriter.Message{
    Key:  "user-42",
    Data: bytes.NewReader([]byte("order-created")),
  })
  if err != nil {
    return err
  }
  ```


  See a detailed example with key routing, alternative strategies (`KafkaHash` and `PartitionID`), and the transactional variant in the [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicwriter/topicwriter_to_many_partitions.go) repository.
- Python

  To send messages, you can pass either just the message content (bytes, str) or manually set some properties. Objects can be passed one at a time or in an array (list). The `write` method is executed asynchronously. The method returns immediately after the messages are placed in the client's internal buffer, which usually happens quickly. Waiting may occur if the internal buffer is already full and you need to wait until some data is sent to the server.

  {% list tabs %}

  - Native SDK

    ```python
    # Simple message sending, without explicit metadata specification.
    # Convenient to start, convenient to use while only the message content matters.
    writer = driver.topic_client.writer(topic_path)
    writer.write("mess")  # Строки будут переданы в кодировке utf-8, так удобно отправлять
                          # text messages.
    writer.write(bytes([1, 2, 3]))  # Эти байты будут отправлены "как есть", так удобно отправлять
                                    # binary data.
    writer.write(["mess-1", "mess-2"])  # Здесь за один вызов отправляется несколько сообщений —
                                         # this reduces overhead on internal SDK processes,
                                         # makes sense with a high message volume.

    # Full form, used when besides the message content you need to manually set its properties.
    writer = driver.topic_client.writer(topic="topic-path", auto_seqno=False, auto_created_at=False)

    writer.write(ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()))
    writer.write(ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now()))

    # In the full form, you can also send multiple messages in one function call.
    # This makes sense with a high volume of sent messages — to reduce
    # overhead on internal SDK calls.
    writer.write([
      ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()),
      ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now(),
      ])
    ```

  - Native SDK (Asyncio)

    ```python
    writer = driver.topic_client.writer(topic_path)
    await writer.write("mess")
    await writer.write(bytes([1, 2, 3]))
    await writer.write(["mess-1", "mess-2"])
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    The `send` method blocks control until the message is placed in the send queue.
    Placing a message in this queue means that the writer will do everything possible to deliver the message.
    For example, if the write session is interrupted for some reason, the writer will re-establish the connection and try to send this message on a new session.
    However, placing a message in the send queue does not guarantee that the message will eventually be written.
    For example, errors may occur that cause the writer to terminate before the messages from the queue are sent.
    If you need confirmation of successful write for each message, use an asynchronous writer and check the status returned by the `send` method.


    ```java
    writer.send(Message.of("11".getBytes()));

    long timeoutSeconds = 5; // How long should we wait for a message to be put into sending buffer
    try {
      writer.send(
              Message.newBuilder()
                      .setData("22".getBytes())
                      .setCreateTimestamp(Instant.now().minusSeconds(5))
                      .build(),
              timeoutSeconds,
              TimeUnit.SECONDS
      );
    } catch (TimeoutException exception) {
      logger.error("Send queue is full. Couldn't put message into sending queue within {} seconds", timeoutSeconds);
    } catch (InterruptedException | ExecutionException exception) {
      logger.error("Couldn't put the message into sending queue due to exception: ", exception);
    }
    ```

  - Asynchronous API

    The `send` method in the asynchronous client is non-blocking. It places the message in the send queue.
    The method returns `CompletableFuture<WriteAck>`, which allows you to check whether the message was actually written.
    If the queue is full, a QueueOverflowException will be thrown.
    This is a way to signal to the user that the write stream should be slowed down.
    In this case, you should either skip messages or perform retries with exponential backoff.
    You can also increase the size of the client buffer (`setMaxSendBufferMemorySize`) to handle a larger volume of messages before it fills up.


    ```java
    try {
      // Non-blocking. Throws QueueOverflowException if send queue is full
      writer.send(Message.of("33".getBytes()));
    } catch (QueueOverflowException exception) {
      // Send queue is full. Need to retry with backoff or skip
    }
    ```

  {% endlist %}
- C#

  Asynchronous writing of a message to a topic.


  ```c#
  var asyncWriteTask = writer.WriteAsync("Hello, Example YDB Topics!"); // Task<WriteResult>
  ```

- JavaScript


  ```javascript
  // Writes a message to the internal buffer
  writer.write(Buffer.from("Hello, world!", "utf-8"));

  // For immediate sending, you need to call flush
  await writer.flush();

  // Or close the writer
  await writer.close();
  ```

- Rust


  ```rust
  use ydb::{TopicWriterMessageBuilder, YdbResult};

  writer
      .write(
          TopicWriterMessageBuilder::default()
              .data(b"payload".to_vec())
              .build()?,
      )
      .await?;
  writer.stop().await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages with confirmation of server-side storage

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    Responses about writing messages on the server come to the SDK client as `TAcksEvent` events. A single event may contain responses about several previously sent messages. Response options: write confirmed (`EES_WRITTEN`), write discarded as a duplicate of a previously written message (`EES_ALREADY_WRITTEN`), or write discarded due to a failure (`EES_DISCARDED`).

    Example of setting up a `TAcksEvent` handler for a write session:


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
      // other settings are set here
      .EventHandlers(
        NYdb::NTopic::TWriteSessionSettings::TEventHandlers()
          .AcksHandler(
            [&](NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
              for (const auto& ack : event.Acks) {
                if (ack.State == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_WRITTEN) {
                  ackedSeqNo.insert(ack.SeqNo);
                  std::cout << "Acknowledged message with seqNo " << ack.SeqNo << std::endl;
                }
              }
            }
          )
      );

    auto session = topicClient.CreateWriteSession(settings);
    ```


    In such a write session, `TAcksEvent` events will not be delivered to the user in `GetEvent` / `GetEvents`; instead, when the SDK receives confirmations from the server, it will call the passed handler. Similarly, you can configure handlers for other event types.

  - ISimpleBlockingWriteSession

    Unlike `IWriteSession`, `ISimpleBlockingWriteSession` does not return confirmations for individual messages: `TAcksEvent` events and their handlers are not available. To wait until all messages from the buffer are written to the server, call `Close()` — the method waits for confirmation from the server and closes the session.

  - IProducer

    Confirmations from the server arrive the same way as for `IWriteSession`: through the `AcksHandler` handler in `TProducerSettings::EventHandlers`. To wait for the accumulated buffer to be delivered to the server, call `Flush()`.


    ```cpp
    auto producerSettings = NYdb::NTopic::TProducerSettings()
        .Path("my-topic")
        .ProducerIdPrefix("my-producer")
        .EventHandlers(
            NYdb::NTopic::TWriteSessionSettings::TEventHandlers()
                .AcksHandler([](NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
                .AcksHandler([](NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) {
                    // handle acknowledgements
                })
                })
        );

    auto producer = topicClient.CreateProducer(producerSettings);
    ```

  {% endlist %}
- Go

  When connecting, you can specify the synchronous message write option - topicoptions.WithSyncWrite(true). Then Write will return only after receiving confirmation from the server that all messages passed in the call have been saved. At the same time, the SDK will, as usual, reconnect and retry sending messages if necessary. In this mode, the context only controls the timeout for the response from the SDK, i.e., even after the context is canceled, the SDK will continue trying to send messages.


  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithSyncWrite(true),
  )

  err = writer.Write(ctx,
    topicwriter.Message{Data: strings.NewReader("1")},
    topicwriter.Message{Data: bytes.NewReader([]byte{1,2,3})},
    topicwriter.Message{Data: strings.NewReader("3")},
  )
  if err != nil {
    return err
  }
  ```

- Python

  There are two ways to get confirmation that messages have been written to the server:

  - `flush()` — waits for confirmation for all messages previously written to the internal buffer.
  - `write_with_ack(...)` — sends a message and waits for confirmation of its delivery from the server. When sending multiple messages in a row, this method works slowly.

  {% list tabs %}

  - Native SDK

    ```python
    # Put multiple messages into the internal buffer, then wait
    # until all are delivered to the server.
    for mess in messages:
        writer.write(mess)

    writer.flush()

    # You can send multiple messages and wait for acknowledgment for the entire group.
    writer.write_with_ack(["mess-1", "mess-2"])

    # Waiting when sending each message — this method will return a result only after receiving
    # acknowledgment from the server.
    # This is the slowest option for sending messages, use it only if such a mode
    # is really needed.
    writer.write_with_ack("message")
    ```

  - Native SDK (Asyncio)

    ```python
    for mess in messages:
        await writer.write(mess)

    await writer.flush()

    await writer.write_with_ack(["mess-1", "mess-2"])
    await writer.write_with_ack("message")
    ```

  {% endlist %}
- Java

  The `send` method returns `CompletableFuture<WriteAck>`. Its successful completion means the write is confirmed by the server.
  The `WriteAck` structure contains information about seqNo, offset, and write status:


  ```java
  writer.send(Message.of(message))
        .whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("Exception on writing message message: ", ex);
            } else {
                switch (result.getState()) {
                    case WRITTEN:
                        WriteAck.Details details = result.getDetails();
                        StringBuilder str = new StringBuilder("Message was written successfully");
                        if (details != null) {
                            str.append(", offset: ").append(details.getOffset());
                        }
                        logger.debug(str.toString());
                        break;
                    case ALREADY_WRITTEN:
                        logger.warn("Message has already been written");
                        break;
                    default:
                        break;
                }
            }
        });
  ```

- C#

  Asynchronous write of a message to a topic. If the internal buffer overflows, it will wait until the buffer is freed for retransmission.


  ```c#
  await writer.WriteAsync("Hello, Example YDB Topics!");
  ```


  If the server is unavailable, messages may accumulate in the queue waiting to be sent. To control the timeout, you can use a cancellation token (`CancellationToken`). However, with this approach, there is a risk that the user may cancel the sending of an already confirmed message.


  ```c#
  var writeCts = new CancellationTokenSource();
  writeCts.CancelAfter(TimeSpan.FromSeconds(3));

  await writer.WriteAsync("Hello, Example YDB Topics!", writeCts.Token);
  ```

- JavaScript

  All messages are written to the internal buffer. There are 3 mechanisms for sending to the server: two automatic and one manual. The manual one is calling the `writer.flush` method, which returns the last seqno written on the server. Automatic sending occurs under the following conditions:

  - Exceeding the internal buffer size `maxBufferBytes` (default value = 256MiB).
  - By the tick of the periodic send interval `flushIntervalMs` (default value = 10ms).


  ```javascript
  await using writer = createTopicWriter(driver, {
    topic: topicName,
    producer: producerName,
    // Callback that is called when writer receives an acknowledgment for a message.
    onAck: (seqNo, status) => {
      console.log("ACK", seqNo, status);
    },
  })

  writer.write(Buffer.from("Hello, world!", "utf-8"));

  // To get the last written seqNo on the server.
  await writer.flush();
  ```

- Rust


  ```rust
  use ydb::{TopicWriterMessageBuilder, YdbResult};

  writer
      .write_with_ack(
          TopicWriterMessageBuilder::default()
              .data(b"payload".to_vec())
              .build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Selecting a codec for message compression {#codec}

Learn more about [data compression in topics](../../concepts/datamodel/topic#message-codec).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    The compression used when sending messages via the `Write` method is set when [creating a write session](#start-writer) with the `Codec` and `CompressionLevel` settings. By default, the GZIP codec is selected.
    Example of creating a write session without message compression:


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
      // other settings are set here
      .Codec(ECodec::RAW);

    auto session = topicClient.CreateWriteSession(settings);
    ```


    If you need to send a message compressed with a different codec within a write session, you can use the `WriteEncoded` method specifying the codec and the size of the uncompressed message. For successful writing using this method, the codec used must be allowed in the topic settings.

  - ISimpleBlockingWriteSession

    The codec is set when [creating a write session](#start-writer) in `TWriteSessionSettings` — the same `Codec` and `CompressionLevel` settings as for `IWriteSession`. The `WriteEncoded` method is not available.


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        // other settings are set here
        .Codec(ECodec::RAW);

    auto session = topicClient.CreateSimpleBlockingWriteSession(settings);
    ```

  - IProducer

    The codec is set in `TProducerSettings` when [creating a producer](#start-writer) — the same `Codec` and `CompressionLevel` settings as for `IWriteSession`.


    ```cpp
    auto producerSettings = NYdb::NTopic::TProducerSettings()
        // other settings are set here
        .Codec(NYdb::NTopic::ECodec::RAW);

    auto producer = topicClient.CreateProducer(producerSettings);
    ```

  {% endlist %}
- Go

  By default, the SDK selects the codec automatically (taking into account the topic settings). In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then occasionally tries to compress messages with all available codecs and selects the codec that gives the smallest message size. If the list of allowed codecs for the topic is empty, the auto-selection is performed between Raw and Gzip codecs.

  If necessary, you can set a fixed codec in the connection options. Then that codec will be used and no measurements will be performed.


  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithCodec(topictypes.CodecGzip),
  )
  ```

- Python

  By default, the SDK selects the codec automatically (taking into account the topic settings). In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then occasionally tries to compress messages with all available codecs and selects the codec that gives the smallest message size. If the list of allowed codecs for the topic is empty, the auto-selection is performed between Raw and Gzip codecs.

  If necessary, you can set a fixed codec in the connection options. Then that codec will be used and no measurements will be performed.


  ```python
  writer = driver.topic_client.writer(topic_path,
      codec=ydb.TopicCodec.GZIP,
  )
  ```

- Java


  ```java
  String producerAndGroupID = "group-id";
  WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .setCodec(Codec.ZSTD)
          .build();
  ```

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript


  ```javascript
  await using writer = t.createWriter({
    codec: Codec.RAW,
  });

  await using writer = t.createWriter({
    codec: Codec.GZIP,
  });

  await using writer = t.createWriter({
    codec: Codec.LZOP,
  });

  await using writer = t.createWriter({
    codec: 10000, // CUSTOM (допустимый диапазон: 10000–19999)
  });
  ```

- Rust

  The choice of compression codec when writing in the Rust SDK is not yet available; messages are sent with the `Raw` codec.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages without deduplication {#nodedup}

For more information about writing without deduplication, see the [corresponding concepts section](../../concepts/datamodel/topic#no-dedup).

{% list tabs group=lang %}

- C++

  {% list tabs %}

  - IWriteSession

    If the `ProducerId` option is not specified in the write session settings, a write session without deduplication will be created.
    Example of creating such a write session:


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path(myTopicPath);

    auto session = topicClient.CreateWriteSession(settings);
    ```


    To enable deduplication, you need to specify the `ProducerId` option in the write session settings or explicitly enable deduplication by calling the `DeduplicationEnabled()` method, for example, as in the ["Connecting to a topic"](#start-writer) section.

  - ISimpleBlockingWriteSession

    The behavior is the same as for `IWriteSession`: if `ProducerId` is not specified, the session is created without deduplication.


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path(myTopicPath);

    auto session = topicClient.CreateSimpleBlockingWriteSession(settings);
    ```

  - IProducer

    `IProducer` always writes with deduplication: the producer identifier is formed from `ProducerIdPrefix` and the partition ID. For writing without deduplication, use `IWriteSession` or `ISimpleBlockingWriteSession`.

  {% endlist %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Go

  In **ydb-go-sdk**, when creating a writer, if `topicoptions.WithWriterProducerID` is not passed, the SDK still substitutes the producer identifier (generates it automatically). The write mode without deduplication, equivalent to the absence of `ProducerId` in the C++ example above, is not available in the current version of the SDK.
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


    ```cpp
    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path(myTopicPath)
    // set all other settings;
    ;

    auto session = topicClient.CreateWriteSession(settings);

    std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);
    NYdb::NTopic::TWriteMessage message("This is yet another message").MessageMeta({
        {"meta-key", "meta-value"},
        {"another-key", "value"}
    });

    if (auto* readyEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
        session->Write(std::move(event.ContinuationToken), std::move(message));
    }
    ```

  - ISimpleBlockingWriteSession

    The same `TWriteMessage` is used as for `IWriteSession`: metadata is set via `MessageMeta()` and passed to `Write()`:


    ```cpp
    auto messageData = std::string("message-data");
    NYdb::NTopic::TWriteMessage writeMessage(messageData);
    writeMessage.MessageMeta({
        {"meta-key", "meta-value"},
        {"another-key", "value"},
    });
    session->Write(std::move(writeMessage));
    ```

  - IProducer

    As with write sessions, metadata is set in `TWriteMessage` via `MessageMeta()`. The difference of `IProducer` is that the message also contains a partitioning key, by which the producer selects the partition:


    ```cpp
    auto messageData = std::string("message-data");
    NYdb::NTopic::TWriteMessage writeMessage("user-42", messageData);
    writeMessage.MessageMeta({
        {"meta-key", "meta-value"},
        {"another-key", "value"},
    });
    producer->Write(std::move(writeMessage));
    ```

  {% endlist %}
- Go

  Metadata is set in the `Metadata` field of the `topicwriter.Message` structure:


  ```go
  err := writer.Write(ctx, topicwriter.Message{
    Data: strings.NewReader("message-data"),
    Metadata: map[string][]byte{
      "meta-key":    []byte("meta-value"),
      "another-key": []byte("value"),
    },
  })
  ```


  When reading, metadata is available in the `Metadata` field of the message:


  ```go
  msg, err := reader.ReadMessage(ctx)
  if err != nil {
    return err
  }
  for k, v := range msg.Metadata {
    fmt.Printf("%s: %s\n", k, string(v))
  }
  ```

- Java

  When constructing a message for writing using a Builder, you can pass it objects of type `MetadataItem` with a key of type `String` and a value of type `byte[]`.

  You can pass `List` such objects at once:


  ```java
  List<MetadataItem> metadataItems = Arrays.asList(
          new MetadataItem("meta-key", "meta-value".getBytes()),
          new MetadataItem("another-key", "value".getBytes())
  );
  writer.send(
          Message.newBuilder()
                  .setMetadataItems(metadataItems)
                  .build()
  );
  ```


  Or add each `MetadataItem` separately:


  ```java
  writer.send(
          Message.newBuilder()
                  .addMetadataItem(new MetadataItem("meta-key", "meta-value".getBytes()))
                  .addMetadataItem(new MetadataItem("another-key", "value".getBytes()))
                  .build()
  );
  ```


  When reading, you can get these message metadata by calling the `getMetadataItems()` method on it:


  ```java
  Message message = reader.receive();
  List<MetadataItem> metadata = message.getMetadataItems();
  ```

- Python

  To use the metadata transfer function, create an `TopicWriterMessage` object with the `metadata_items` argument, as shown below:

  {% list tabs %}

  - Native SDK

    ```python
    message = ydb.TopicWriterMessage(data=f"message-data", metadata_items={"meta-key": "meta-value"})
    writer.write(message)
    ```

  - Native SDK (Asyncio)

    ```python
    message = ydb.TopicWriterMessage(data="message-data", metadata_items={"meta-key": "meta-value"})
    await writer.write(message)
    ```

  {% endlist %}

  During reading, metadata can be obtained from the `metadata_items` field of the `PublicMessage` object:


  ```python
  message = reader.receive_message()
  for meta_key, meta_value in message.metadata_items.items():
      print(f"{meta_key}: {meta_value}")
  ```

- C#


  ```c#
  await writer.WriteAsync(
      new Ydb.Sdk.Services.Topic.Writer.Message<string>("Hello, Example YDB Topics!")
          { Metadata = { new Metadata("meta-key", "meta-value"u8.ToArray()) } }
  );
  ```

- JavaScript


  ```javascript
  writer.write(Buffer.from("Hello, world!", "utf-8"), {
    metadataItems: {
      "meta-key": new TextEncoder().encode("meta-value"),
    },
  });
  ```

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

    [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/examples/topic_writer/transaction/main.cpp)


    ```c++
    NYdb::NQuery::TQueryClient queryClient(driver);

    NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        auto beginTxResult = session.BeginTransaction().GetValueSync();
        if (!beginTxResult.IsSuccess()) {
            return beginTxResult;
        }
        auto tx = beginTxResult.GetTransaction();

        NYdb::NTopic::TWriteMessage writeMessage("message");

        topicSession->Write(std::move(writeMessage), tx);
        return tx.Commit().GetValueSync();
    }));
    ```

  - ISimpleBlockingWriteSession

    Like `IWriteSession`, `ISimpleBlockingWriteSession` supports writing in transactions. Since the simple variant does not have `ContinuationToken`, the transaction object is passed as the second argument to `Write()`.

    [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/examples/topic_writer/transaction/main.cpp)


    ```c++
    NYdb::NQuery::TQueryClient queryClient(driver);

    NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        auto beginTxResult = session.BeginTransaction().GetValueSync();
        if (!beginTxResult.IsSuccess()) {
            return beginTxResult;
        }
        auto tx = beginTxResult.GetTransaction();

        NYdb::NTopic::TWriteMessage writeMessage("message");

        topicSession->Write(std::move(writeMessage), &tx);
        return tx.Commit().GetValueSync();
    }));
    ```

  - IProducer

    For `IProducer`, the transaction is specified not by the `Write` argument, but in `TWriteMessage` via `Tx()`. After that, the producer writes the message in the same way as a regular message with a partitioning key:

  {% endlist %}
- Go

  To write to a topic in a transaction, you need to create a transactional writer by calling [TopicClient.StartTransactionalWriter](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic#Client.StartTransactionalWriter). After that, you can send messages as usual. There is no need to close the transactional writer — it happens automatically when the transaction completes.

  [Example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicwriter/topic_writer_transaction.go)


  ```go
  err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
    writer, err := db.Topic().StartTransactionalWriter(tx, topicName)
    if err != nil {
      return err
    }

    return writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")})
  })
  ```

- Python

  To write to a topic in a transaction, you need to create a transactional writer by calling `topic_client.tx_writer`. After that, you can send messages as usual. There is no need to close the transactional writer — it happens automatically when the transaction completes.

  In the example below, there is no explicit call to `tx.commit()` — it happens implicitly upon successful completion of the `callee` lambda.

  [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/topic/topic_transactions_example.py)

  {% list tabs %}

  - Native SDK

    ```python
    with ydb.QuerySessionPool(driver) as session_pool:

        def callee(tx: ydb.QueryTxContext):
            tx_writer: ydb.TopicTxWriter = driver.topic_client.tx_writer(tx, topic)

            for i in range(message_count):
                result_stream = tx.execute(query=f"select {i} as res;")
                for result_set in result_stream:
                    message = str(result_set.rows[0]["res"])
                    tx_writer.write(ydb.TopicWriterMessage(message))
                    print(f"Message {message} was written with tx.")

        session_pool.retry_tx_sync(callee)
    ```

  - Native SDK (Asyncio)

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/topic/topic_transactions_async_example.py)


    ```python
    async with ydb.aio.QuerySessionPool(driver) as session_pool:

        async def callee(tx: ydb.aio.QueryTxContext):
            tx_writer: ydb.TopicTxWriterAsyncIO = driver.topic_client.tx_writer(tx, topic)

            for i in range(message_count):
                async with await tx.execute(query=f"select {i} as res;") as result_stream:
                    async for result_set in result_stream:
                        message = str(result_set.rows[0]["res"])
                        await tx_writer.write(ydb.TopicWriterMessage(message))
                        print(f"Message {result_set.rows[0]['res']} was written with tx.")

        await session_pool.retry_tx_async(callee)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteSync.java)

    In the settings of the `SendSettings` method `send`, you can specify a transaction.
    Then the message will be written together with the commit of that transaction.


    ```java
    // creating a session in the table service
    Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
    if (!sessionResult.isSuccess()) {
      logger.error("Couldn't get a session from the pool: {}", sessionResult);
      return; // retry or shutdown
    }
    Session session = sessionResult.getValue();
    // creating a transaction in the table service
    // this transaction is not yet active and has no id
    TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

    // get message text within the transaction
    Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
          .join();
    if (!dataQueryResult.isSuccess()) {
      logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
      return; // retry or shutdown
    }
    // now the transaction is active and has an id

    ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
    byte[] message;
    if (rsReader.next()) {
      message = rsReader.getColumn(0).getBytes();
    } else {
      return; // retry or shutdown
    }

    writer.send(
          Message.of(message),
          SendSettings.newBuilder()
                  .setTransaction(transaction)
                  .build()
    );

    // flush to wait until all messages reach server before commit
    writer.flush();

    Status commitStatus = transaction.commit().join();
    analyzeCommitStatus(commitStatus);
    ```

  - Asynchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteAsync.java)

    In the settings of the `SendSettings` method `send`, you can specify a transaction.
    Then the message will be written together with the commit of that transaction.


    ```java
    // creating a session in the table service
    Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
    if (!sessionResult.isSuccess()) {
      logger.error("Couldn't get a session from the pool: {}", sessionResult);
      return; // retry or shutdown
    }
    Session session = sessionResult.getValue();
    // creating a transaction in the table service
    // this transaction is not yet active and has no id
    TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

    // get message text within the transaction
    Result<DataQueryResult> dataQueryResult = transaction.executeDataQuery("SELECT \"Hello, world!\";")
          .join();
    if (!dataQueryResult.isSuccess()) {
      logger.error("Couldn't execute DataQuery: {}", dataQueryResult);
      return; // retry or shutdown
    }
    // now the transaction is active and has an id

    ResultSetReader rsReader = dataQueryResult.getValue().getResultSet(0);
    byte[] message;
    if (rsReader.next()) {
      message = rsReader.getColumn(0).getBytes();
    } else {
      return; // retry or shutdown
    }

    try {
      writer.send(Message.newBuilder()
                              .setData(message)
                              .build(),
                      SendSettings.newBuilder()
                              .setTransaction(transaction)
                              .build())
              .whenComplete((result, ex) -> {
                  if (ex != null) {
                      logger.error("Exception while sending a message: ", ex);
                  } else {
                      switch (result.getState()) {
                          case WRITTEN:
                              WriteAck.Details details = result.getDetails();
                              logger.info("Message was written successfully, offset: " + details.getOffset());
                              break;
                          case ALREADY_WRITTEN:
                              logger.info("Message has already been written");
                              break;
                          default:
                              break;
                      }
                  }
              })
              // Waiting for the message to reach the server before committing the transaction
              .join();

      Status commitStatus = transaction.commit().join();
      analyzeCommitStatus(commitStatus);
    } catch (QueueOverflowException exception) {
      logger.error("Queue overflow exception while sending a message{}: ", index, exception);
      // Send queue is full. Need to retry with backoff or skip
    }
    ```

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for Rust SDK support: [ydb-rs-sdk#341](https://github.com/ydb-platform/ydb-rs-sdk/issues/341)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Reading messages {#reading}

### Connecting to a topic for reading messages {#start-reader}

Reading messages from a topic can be performed with a Consumer associated with that topic, or without a Consumer. If no Consumer is specified, the client application must calculate the offset for reading messages on its own. A more detailed example of reading without a Consumer is discussed in the [corresponding section](#no-consumer).

You can create a Consumer when [creating](#create-topic) or [modifying](#alter-topic) a topic.
A topic can have multiple Consumers, and the server stores its own read progress for each of them.

{% list tabs group=lang %}

- C++

  A connection for reading from one or more topics is represented by a read session object with the `IReadSession` interface. The read session settings are represented by the `TReadSessionSettings` structure.

  See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic");

  auto session = topicClient.CreateReadSession(settings);
  ```

- Go

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:


  ```go
  reader, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"))
  if err != nil {
      return err
  }
  ```

- Python

  To create a connection to an existing topic `my-topic` through a previously added reader `my-consumer`, use the following code:

  {% list tabs %}

  - Native SDK

    ```python
    reader = driver.topic_client.reader(topic="my-topic", consumer="my-consumer")
    ```

  - Native SDK (Asyncio)

    ```python
    reader = driver.topic_client.reader(topic="my-topic", consumer="my-consumer")
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Initializing reader settings


    ```java
    ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)  // имя consumer'а, зарегистрированного на топике
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // читать с этой временной метки (опционально)
                  .setMaxLag(Duration.ofMinutes(30)) // максимальное отставание от конца очереди (опционально)
                  .build())
          .build();
    ```


    Creating a synchronous reader


    ```java
    SyncReader reader = topicClient.createSyncReader(settings);
    ```


    After creating a synchronous reader, you need to initialize it. To do this, use one of two methods:

    - `init()`: non-blocking, starts the initialization process in the background and does not wait for it to complete.


    ```java
    reader.init();
    ```


    - `initAndWait()`: blocking, starts the initialization process and waits for it to complete. If an error occurs during initialization, an exception will be thrown.


    ```java
    try {
        reader.initAndWait();
        logger.info("Init finished successfully");
    } catch (Exception exception) {
        logger.error("Exception while initializing reader: ", exception);
        return;
    }
    ```

  - Asynchronous API

    Initializing reader settings


    ```java
    ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)  // имя consumer'а, зарегистрированного на топике
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // читать с этой временной метки (опционально)
                  .setMaxLag(Duration.ofMinutes(30)) // максимальное отставание от конца очереди (опционально)
                  .build())
          .build();
    ```


    For an asynchronous reader, in addition to the general read settings `ReaderSettings`, you will need the event handler settings `ReadEventHandlersSettings`, in which you must pass an instance of a subclass `ReadEventHandler`.
    It will describe how to handle various events that occur during reading.


    ```java
    ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
          .setEventHandler(new Handler())
          .build();
    ```


    Optionally, in `ReadEventHandlersSettings` you can specify an executor on which message processing will occur; by default, the internal SDK thread is used.

    To implement an event handler, you can inherit from `AbstractReadEventHandler` and override the `onMessages` method.
    The `onMessages` method is called each time the SDK receives the next batch of messages from the server. Within a single call, one or more messages arrive, which can be acknowledged (`commit`) either individually or after processing the entire batch. Example implementation:


    ```java
    private class Handler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (Message message : event.getMessages()) {
              StringBuilder str = new StringBuilder();
              logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());

              process(message);

              message.commit().thenRun(() -> {
                  logger.info("Message committed");
              });
          }
      }
    }
    ```


    Creating and initializing an asynchronous reader:


    ```java
    AsyncReader reader = topicClient.createAsyncReader(readerSettings, handlerSettings);
    // Init in background
    reader.init()
          .thenRun(() -> logger.info("Init finished successfully"))
          .exceptionally(ex -> {
              logger.error("Init failed with ex: ", ex);
              return null;
          });
    ```

  {% endlist %}
- C#


  ```c#
  await using var reader = new ReaderBuilder<string>(connectionString)
  {
      ConsumerName = "Consumer_Example",
      SubscribeSettings = { new SubscribeSettings(topicName) }
  }.Build();
  ```

- JavaScript


  ```javascript
  await using reader = createTopicReader(driver, {
    topic: topicName,
    consumer: consumerName,
  });
  ```

- Rust


  ```rust
  use ydb::YdbResult;

  let mut reader = topic_client
      .create_reader("my-consumer", "/local/my-topic")
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

You can also use an extended connection creation option to specify multiple topics and set reading parameters. The following code will create a connection to topics `my-topic` and `my-specific-topic` via reader `my-consumer`:

{% list tabs group=lang %}

- C++


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic")
      .AppendTopics(
          NYdb::NTopic::TTopicReadSettings("my-specific-topic")
              .ReadFromTimestamp(someTimestamp)
      );

  auto session = topicClient.CreateReadSession(settings);
  ```

- Go


  ```go
  reader, err := db.Topic().StartReader("my-consumer", []topicoptions.ReadSelector{
      {
          Path: "my-topic",
      },
      {
          Path:       "my-specific-topic",
          ReadFrom:   time.Date(2022, 7, 1, 10, 15, 0, 0, time.UTC),
      },
      },
  )
  if err != nil {
      return err
  }
  ```


  Also, the example above sets the time from which to start reading messages.
- Python

  This functionality is under development.
- Java


  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath("my-topic")
                  .build())
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath("my-specific-topic")
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
                  .build())
          .build();
  ```

- C#


  ```c#
  await using var reader = new ReaderBuilder<string>(connectionString)
  {
      ConsumerName = "Consumer_Example",
      SubscribeSettings =
      {
          new SubscribeSettings(topicName),
          new SubscribeSettings(topicName + "_another") { ReadFrom = DateTime.Now }
      }
  }.Build();
  ```

- JavaScript


  ```javascript
  await using reader = createTopicReader(driver, {
    topic: {
      path: topicPath,
      partitionIds: [1n, 2n, 3n],
    },
    consumer: consumerName,
  });

  await using reader = createTopicReader(driver, {
    topic: {
      path: topicPath,
      maxLag: "1s", // number, import('ms').StringValue, protobuff Duration
    },
    consumer: consumerName,
  });

  await using reader = createTopicReader(driver, {
    topic: {
      path: topicPath,
      readFrom: new Date(), // number, Date, protobuf Timestamp
    },
    consumer: consumerName,
  });

  await using reader = createTopicReader(driver, {
    topic: [
      {
        path: topicPath,
        partitionIds: [1n, 2n, 3n],
      },
      {
        path: topicPath2,
        maxLag: "1s",
      },
      {
        path: topicPath3,
        readFrom: new Date(),
      },
      // ...
    ],
    consumer: consumerName,
  });
  ```

- Rust


  ```rust
  use std::time::{Duration, SystemTime};

  use ydb::{TopicReaderOptionsBuilder, TopicSelector, TopicSelectors, YdbResult};

  let mut reader = topic_client
      .create_reader_with_params(
          TopicReaderOptionsBuilder::default()
              .consumer("my-consumer".into())
              .topic(TopicSelectors(vec![
                  TopicSelector::new("/local/my-topic"),
                  TopicSelector {
                      path: "/local/my-specific-topic".into(),
                      partition_ids: None,
                      read_from: Some(SystemTime::now() - Duration::from_secs(3600)),
                  },
              ]))
              .build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [message read position](../../concepts/datamodel/topic.md#consumer-offset). After reading the next message, the client can [send a processing acknowledgment to the server](#commit). The read position will change, and upon a new connection, only unacknowledged messages will be read.

You can also read messages [without processing acknowledgment](#no-commit). In this case, upon a new connection, all unacknowledged messages will be read, including those already processed.

Information about which messages have already been processed can be [stored on the client side](#client-commit) by passing the starting read position to the server when creating a connection. In this case, the message read position on the server does not change.

You can use [transactions](#read-tx). In this case, the read position will change when the transaction is committed. Upon a new connection, all unacknowledged messages will be read.

{% list tabs group=lang %}

- C++

  The user's work with the `IReadSession` object is generally organized as processing an event loop with the following event types: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TEndPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent`, and `TSessionClosedEvent`.

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

  A complete example of reading a topic in a transaction with writing to a table: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).


  ```rust
  let batch = reader.pop_batch_in_tx(&mut tx).await?;
  // processing batch.messages and committing transaction
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without message processing acknowledgment {#no-commit}

#### Reading messages one by one

{% list tabs group=lang %}

- C++

  Reading messages one by one is not supported in the C++ SDK. The `TDataReceivedEvent` event contains a batch of messages.
- Go


  ```go
  func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
      for {
          mess, err := r.ReadMessage(ctx)
          if err != nil {
              return err
          }
          processMessage(mess)
      }
  }
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    while True:
        message = reader.receive_message()
        process(message)
    ```

  - Native SDK (Asyncio)

    ```python
    while True:
        message = await reader.receive_message()
        process(message)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    To read messages one by one without processing acknowledgment, use the following code:


    ```java
    while(true) {
      Message message = reader.receive();
      process(message);
    }
    ```

  - Asynchronous API

    In the asynchronous client, it is not possible to read messages one by one.

  {% endlist %}
- C#


  ```c#
  try
  {
      while (!readerCts.IsCancellationRequested)
      {
          var message = await reader.ReadAsync(readerCts.Token);

          logger.LogInformation("Received message: [{MessageData}]", message.Data);
      }
  }
  catch (OperationCanceledException)
  {
  }
  ```

- JavaScript


  ```javascript
  for await (let batch of reader.read()) {
    for await (let msg of batch) {
    }
  }
  ```

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


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << NYdb::NTopic::DebugString(event);
          }
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  session->GetEvent(/* block = */true);
  ```


  In this example, after creating the session, the main thread waits for the session to be closed by the server in the `GetEvent` method; other event types will not arrive.
- Go


  ```go
  func SimpleReadBatches(ctx context.Context, r *topicreader.Reader) error {
      for {
          batch, err := r.ReadMessageBatch(ctx)
          if err != nil {
              return err
          }
          processBatch(batch)
      }
  }
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    while True:
        batch = reader.receive_batch()
        process(batch)
    ```

  - Native SDK (Asyncio)

    ```python
    while True:
        batch = await reader.receive_batch()
        process(batch)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    In the synchronous client, it is not possible to read a batch of messages at once.

  - Asynchronous API

    To read a batch of messages without processing acknowledgment, use the following code:


    ```java
    private class Handler extends AbstractReadEventHandler {
      @Override
      public void onMessages(DataReceivedEvent event) {
          for (Message message : event.getMessages()) {
              process(message);
          }
      }
    }
    ```

  {% endlist %}
- C#


  ```c#
  try
  {
      while (!readerCts.IsCancellationRequested)
      {
          var batchMessages = await reader.ReadBatchAsync(readerCts.Token);

          foreach (var message in batchMessages.Batch)
          {
              logger.LogInformation("Received message: [{MessageData}]", message.Data);
          }
      }
  }
  catch (OperationCanceledException)
  {
  }
  ```

- JavaScript


  ```javascript
  for await (let batch of reader.read()) {
  }
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with message processing acknowledgment {#commit}

Message processing acknowledgment (commit) tells the server that the message from the topic has been processed by the receiver and no longer needs to be sent. When using reading with acknowledgment, you must acknowledge all received messages without skipping. Message commit on the server occurs after acknowledging the next interval of messages "without holes"; the acknowledgments themselves can be sent in any order.

For example, messages 1, 2, 3 arrive from the server. The program processes them in parallel and sends acknowledgments in this order: 1, 3, 2. In this case, message 1 will be committed first, and messages 2 and 3 will be committed only after the server receives acknowledgment of message 2 processing.

If an error occurs on a message commit, you can log the error and continue working. The state of the message at this point is unknown. The message could have been committed, and then a network error occurred and the client did not receive the acknowledgment. If the message was not committed, it will be read again and will be processed again (possibly by a different reader). There is no point in retrying the commit itself, because the read session for this message is already lost.

#### Reading messages one by one with acknowledgment

{% list tabs group=lang %}

- C++

  Reading messages one by one is not supported in the C++ SDK. The `TDataReceivedEvent` event contains a batch of messages.
- Go


  ```go
  func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
      for {
        mess, err := r.ReadMessage(ctx)
        if err != nil {
            return err
        }
        processMessage(mess)
        r.Commit(mess.Context(), mess)
      }
  }
  ```


  By default, `Commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, you need to explicitly close the reader by calling `Reader.Close()`.
- Python

  `commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, you need to explicitly close the reader.

  {% list tabs %}

  - Native SDK

    ```python
    while True:
        message = reader.receive_message()
        process(message)
        reader.commit(message)
    ```

  - Native SDK (Asyncio)

    ```python
    while True:
        message = await reader.receive_message()
        process(message)
        reader.commit(message)
    ```

  {% endlist %}
- Java

  To confirm message processing, call the `commit` method on the message.
  This applies to both synchronous and asynchronous readers.
  In an asynchronous reader, when processing a batch of messages, you can call `commit` either on the entire batch at once or on each message individually.
  This method returns `CompletableFuture<Void>`; its successful completion means that the server has confirmed the processing.
  In case of a commit error, do not attempt to retry it. Most likely, the error is caused by session closure.
  The reader (not necessarily the same one) will create a new session for this partition, and the message will be read again.


  ```java
  message.commit()
         .whenComplete((result, ex) -> {
             if (ex != null) {
                 // Read session was probably closed, there is nothing we can do here.
                 // Do not retry this commit on the same event.
                 logger.error("exception while committing message: ", ex);
             } else {
                 logger.info("message committed successfully");
             }
         });
  ```

- C#


  ```c#
  try
  {
      while (!readerCts.IsCancellationRequested)
      {
          var message = await reader.ReadAsync(readerCts.Token);

          logger.LogInformation("Received message: [{MessageData}]", message.Data);

          try
          {
              await message.CommitAsync();
          }
          catch (ReaderException e)
          {
              logger.LogError(e, "Failed to commit a message");
          }
      }
  }
  catch (OperationCanceledException)
  {
  }
  ```

- JavaScript


  ```javascript
  for await (let batch of reader.read()) {
    for (let msg of batch) {
      await reader.commit(msg);
    }
  }
  ```

- Rust


  ```rust
  let batch = reader.read_batch().await?;
  reader.commit(batch.get_commit_marker())?;
  // or with waiting for ack from the server:
  reader.commit_with_ack(batch.get_commit_marker()).await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Reading messages in a batch with acknowledgment

{% list tabs group=lang %}

- C++

  Similarly to the [example above](#no-commit), when setting up a read session with the `SimpleDataHandlers` setting, it is enough to pass a handler for data messages. The SDK will call this handler for each message packet received from the server. Passing the `commitDataAfterProcessing = true` parameter means that the SDK will send read acknowledgments for all messages to the server after the handler is executed.


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << NYdb::NTopic::DebugString(event);
          }
          , /* commitDataAfterProcessing = */true
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  session->GetEvent(/* block = */true);
  ```

- Go


  ```go
  func SimpleReadMessageBatch(ctx context.Context, r *topicreader.Reader) error {
      for {
        batch, err := r.ReadMessageBatch(ctx)
        if err != nil {
            return err
        }
        processBatch(batch)
        r.Commit(batch.Context(), batch)
      }
  }
  ```


  By default, `Commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the latest commits before exiting the program, the reader must be closed explicitly.
- Python

  {% list tabs %}

  - Native SDK

    ```python
    while True:
        batch = reader.receive_batch()
        process(batch)
        reader.commit(batch)
    ```

  - Native SDK (Asyncio)

    ```python
    while True:
        batch = await reader.receive_batch()
        process(batch)
        reader.commit(batch)
    ```

  {% endlist %}

  `commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the latest commits before exiting the program, the reader must be closed explicitly.
- Java

  {% list tabs %}

  - Synchronous API

    Not relevant, because the synchronous reader does not support reading messages in batches.

  - Asynchronous API

    In the `onMessages` handler, you can commit the entire message batch by calling `commit` on the event.


    ```java
    @Override
    public void onMessages(DataReceivedEvent event) {
      for (Message message : event.getMessages()) {
          process(message);
      }
      event.commit()
             .whenComplete((result, ex) -> {
                 if (ex != null) {
                     // Read session was probably closed, there is nothing we can do here.
                     // Do not retry this commit on the same message.
                     logger.error("exception while committing message batch: ", ex);
                 } else {
                     logger.info("message batch committed successfully");
                 }
             });
    }
    ```

  {% endlist %}
- C#


  ```c#
  try
  {
      while (!readerCts.IsCancellationRequested)
      {
          var batchMessages = await reader.ReadBatchAsync(readerCts.Token);

          foreach (var message in batchMessages.Batch)
          {
              logger.LogInformation("Received message: [{MessageData}]", message.Data);
          }

          try
          {
              await batchMessages.CommitBatchAsync();
          }
          catch (ReaderException e)
          {
              logger.LogError(e, "Failed to commit a message");
          }
      }
  }
  catch (OperationCanceledException)
  {
  }
  ```

- JavaScript


  ```javascript
  for await (let batch of reader.read()) {
    await reader.commit(batch);
  }
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with position storage on the client side {#client-commit}

Instead of committing messages to the server, you can store the read progress yourself. In this case, you need to pass a handler to the SDK that will be called when starting to read each partition. In this handler, you will need to specify the position from which to start reading this partition.

{% list tabs group=lang %}

- C++

  When processing events `TStartPartitionSessionEvent`, you can set the position from which to start reading in the response to the server. To do this, you should pass to the method `Confirm` the parameter `readOffset`. Additionally, you can pass the parameter `commitOffset`, which will specify the position up to which messages should be considered [committed](#commit).

  Example of setting up a handler:


  ```cpp
  settings.EventHandlers_.StartPartitionSessionHandler(
      [](NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
          auto readFromOffset = GetOffsetToReadFrom(event.GetPartitionId());
          event.Confirm(readFromOffset);
      }
  );
  ```


  Here `GetOffsetToReadFrom` is part of the example, not the SDK. Use your own method to determine the required starting read position for a partition with the given partition id.

  Also, in `TReadSessionSettings`, the `ReadFromTimestamp` setting is supported for reading events with write timestamps not less than the specified one. This setting is intended not for precise start positioning, but for skipping a volume of data over a large time interval. The first few received messages may have write timestamps less than the specified one.
- Go

  {% note tip %}

  In the default reader mode, offsets up to the position specified via `res.StartFrom` are committed on the server. After that, re-reading the same messages by moving the position back becomes impossible. To disable auto-commit, use the no-commit mode when creating the reader.


  ```go
  reader, err := db.Topic().StartReader(
    consumerName,
    topicoptions.ReadTopic(topicName),
    topicoptions.WithReaderCommitMode(topicoptions.CommitModeNone),
  )
  ```

  {% endnote %}


  ```go
  func ReadWithExplicitPartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db ydb.Connection) error {
      readContext, stopReader := context.WithCancel(context.Background())
      defer stopReader()

      readStartPosition := func(
          ctx context.Context,
          req topicoptions.GetPartitionStartOffsetRequest,
      ) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
          offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
          res.StartFrom(offset)

          // Reader will stop if return err != nil
          return res, err
      }

      r, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"),
          topicoptions.WithGetPartitionStartOffset(readStartPosition),
      )
      if err != nil {
          return err
      }

      go func() {
          <-readContext.Done()
          _ = r.Close(ctx)
      }()

      for {
          batch, err := r.ReadMessageBatch(readContext)
          if err != nil {
              return err
          }

          processBatch(batch)
          _ = externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), batch.EndOffset())
      }
  }
  ```

- Python

  The functionality is under development.
- Java

  Reading from a specified offset in Java is only possible in an asynchronous reader. In the `StartPartitionSessionEvent` event handler, you can specify the position from which to start reading when responding to the server. To do this, pass the `StartPartitionSessionSettings` settings with the specified offset to the `confirm` method via `setReadOffset`. Also, by calling `setCommitOffset`, you can specify the offset that should be considered committed.


  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // Long
              .setCommitOffset(lastCommitOffset) // Long
              .build());
  }
  ```


  Also supported is the configuration of the `setReadFrom` reader to read events with write timestamps not less than the given one.
- JavaScript


  ```javascript
  await using reader = createTopicReader(driver, {
    topic: topicName,
    consumer: consumerName,
    onPartitionSessionStart: (evt) => {
      return {
        readOffset: 0n,
        commitOffset: 0n,
      };
    },
  });
  ```

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without specifying a Consumer {#no-consumer}

Typically, topic read progress is saved on the server in each `Consumer`. However, you can choose not to store such progress on the server and explicitly specify when creating a reader that reading will occur without `Consumer`.

{% list tabs group=lang %}

- C++

  In `NYdb::NTopic::TReadSessionSettings`, call `WithoutConsumer()`:


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .WithoutConsumer()
      .AppendTopics(
          NYdb::NTopic::TTopicReadSettings("topic-path")
              .AppendPartitionIds(0)
              .AppendPartitionIds(1)
              .AppendPartitionIds(2));

  auto readSession = topicClient.CreateReadSession(settings);
  ```


  Upon reconnection, read progress is not saved on the server. To avoid starting from the beginning, pass the offset in `TStartPartitionSessionEvent::Confirm` at each start of a partition read session — see [storing position on the client](#client-commit).
- Go

  You need to pass an empty string as the consumer name and the `topicoptions.WithReaderWithoutConsumer(false)` option (**experimental** mode, see [VERSIONING](https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md) in the SDK repository). In the read selector, specify the topic path and the list of partitions. Message commits are not available in this mode (`CommitModeNone`); on reconnections, progress must be restored on the client side — see [storing position on the client](#client-commit).


  ```go
  reader, err := db.Topic().StartReader(
    "",
    topicoptions.ReadSelectors{{
      Path:       "topic-path",
      Partitions: []int64{0, 1, 2},
    }},
    topicoptions.WithReaderWithoutConsumer(false),
  )
  if err != nil {
    return err
  }
  ```

- Java

  To read without a consumer, you should explicitly specify this in the reader settings `ReaderSettings` by calling `withoutConsumer()`:


  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .withoutConsumer()
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(TOPIC_NAME)
                  .build())
          .build();
  ```


  In this case, note that when the connection is re-established, the progress on the server will be reset. Therefore, to avoid starting reading from the beginning, you should pass the starting read offset in the SDK at each start of a partition read session:


  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // the last offset read by this client, Long
              .build());
  }
  ```

- Python

  To read without a consumer, create a reader using the `reader` method with the following arguments:

  - `topic` — an `ydb.TopicReaderSelector` object with the specified `path` and list of `partitions`;
  - `consumer` — must be `None`;
  - `event_handler` — a descendant of `ydb.TopicReaderEvents.EventHandler` that implements the `on_partition_get_start_offset` function. This function is responsible for returning the initial offset (offset) for reading messages when the reader starts, as well as during reconnections. The client application must specify this offset in the `ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse.start_offset` parameter. The function can also be implemented as asynchronous.

  Example:


  ```python
  class CustomEventHandler(ydb.TopicReaderEvents.EventHandler):
      def on_partition_get_start_offset(self, event: ydb.TopicReaderEvents.OnPartitionGetStartOffsetRequest):
          return ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse(
              start_offset=0,
          )

  reader = driver.topic_client.reader(
      topic=ydb.TopicReaderSelector(
          path="topic-path",
          partitions=[0, 1, 2],
      ),
      consumer=None,
      event_handler=CustomEventHandler(),
  )
  ```

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

  Before reading from a topic, the client code must pass a reference to the transaction object into the session event receiving settings.

  [Example on GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/examples/topic_reader/transaction/application.cpp)


  ```cpp
  readSession->WaitEvent().Wait(TDuration::Seconds(1));

  NYdb::NStatusHelpers::ThrowOnError(queryClient.RetryQuerySync([&readSession](NYdb::NQuery::TSession session) -> NYdb::TStatus {
      auto beginTxResult = session.BeginTransaction(NYdb::Query::TTxSettings::SerializableRW()).GetValueSync();
      if (!beginTxResult.IsSuccess()) {
          return beginTxResult;
      }
      auto tx = beginTxResult.GetTransaction();

      auto topicSettings = NYdb::NTopic::TReadSessionGetEventSettings()
          .Block(false);
          .Tx(tx);

      auto events = readSession->GetEvents(topicSettings);

      for (auto& event : events) {
          // process the event and write results to the table
      }

      return tx.Commit().GetValueSync();
  }));
  ```


  {% note warning %}

  When processing `events` events, you do not need to explicitly acknowledge processing for events of type `TDataReceivedEvent`.

  {% endnote %}

  Acknowledgment of `TStopPartitionSessionEvent` event processing must be done after calling `Commit`.


  ```cpp
  std::optional<NYdb::NTopic::TStopPartitionSessionEvent> stopPartitionSession;

  auto events = readSession->GetEvents(topicSettings);

  for (auto& event : events) {
      if (auto* e = std::get_if<NYdb::NTopic::TStopPartitionSessionEvent>(&event)) {
          stopPartitionSessionEvent = std::move(*e);
      } else {
          // process the event and write results to the table
      }
  }

  auto commitResult = tx.Commit(commitSettings).GetValueSync();
  if (!commitResult.IsSuccess()) {
      return commitResult;
  }

  if (stopPartitionSessionEvent) {
      stopPartitionSessionEvent->Commit();
  }
  ```

- Go

  To read messages within a transaction, use the [`Reader.PopMessagesBatchTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader#Reader.PopMessagesBatchTx) method. It reads a batch of messages and adds their commit to the transaction; you do not need to commit these messages separately. The message reader can be reused in different transactions. It is important that the order of transaction commits matches the order of receiving messages from the reader, because message commits in a topic must be performed strictly in order. The easiest way to do this is to use the reader in a loop.

  [Example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicreader/topic_reader_transaction.go)


  ```go
  for {
    err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
      batch, err := reader.PopMessagesBatchTx(ctx, tx) // батч закоммитится при общем коммите транзакции
      if err != nil {
        return err
      }

      return processBatch(ctx, batch)
    })
    if err != nil {
      handleError(err)
    }
  }
  ```

- Python

  To read messages within a transaction, use the `reader.receive_batch_with_tx` method. It reads a batch of messages and adds their commit to the transaction; you do not need to commit these messages separately. The message reader can be reused in different transactions. It is important that the order of transaction commits matches the order of receiving messages from the reader, because message commits in a topic must be performed strictly in order — otherwise, the transaction will get an error when attempting to commit. The easiest way to do this is to use the reader in a loop.

  {% list tabs %}

  - Native SDK

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/topic/topic_transactions_example.py)


    ```python
    with driver.topic_client.reader(topic, consumer) as reader:
        with ydb.QuerySessionPool(driver) as session_pool:
            for _ in range(message_count):

                def callee(tx: ydb.QueryTxContext):
                    batch = reader.receive_batch_with_tx(tx, max_messages=1)
                    print(f"Message {batch.messages[0].data.decode()} was read with tx.")

                session_pool.retry_tx_sync(callee)
    ```

  - Native SDK (Asyncio)

    [Example on GitHub](https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/topic/topic_transactions_async_example.py)


    ```python
    async with driver.topic_client.reader(topic, consumer) as reader:
        async with ydb.aio.QuerySessionPool(driver) as session_pool:
            for _ in range(message_count):

                async def callee(tx: ydb.aio.QueryTxContext):
                    batch = await reader.receive_batch_with_tx(tx, max_messages=1)
                    print(f"Message {batch.messages[0].data.decode()} was read with tx.")

                await session_pool.retry_tx_async(callee)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadSync.java)

    In the `ReceiveSettings` settings of the `receive` method, you can specify a transaction:


    ```java
    Message message = reader.receive(ReceiveSettings.newBuilder()
          .setTransaction(transaction)
          .build());
    ```


    Then the received message will be committed together with the transaction. You do not need to commit it separately.
    The `receive` method will link the message offsets with the transaction on the server by calling `sendUpdateOffsetsInTransaction` and return control when it receives a response.

  - Asynchronous API

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadAsync.java)

    After receiving a message in the `onMessages` handler, you can associate one or more messages with a transaction.
    To do this, call a separate method `reader.updateOffsetsInTransaction` and wait for its execution on the server.
    This method takes a list of offsets as a parameter. For convenience, `Message` and `DataReceivedEvent` have a method `getPartitionOffsets()` that returns such a list.


    ```java
    @Override
    public void onMessages(DataReceivedEvent event) {
      for (Message message : event.getMessages()) {
          // creating a session in the table service
          Result<Session> sessionResult = tableClient.createSession(Duration.ofSeconds(10)).join();
          if (!sessionResult.isSuccess()) {
              logger.error("Couldn't get a session from the pool: {}", sessionResult);
              return; // retry or shutdown
          }
          Session session = sessionResult.getValue();
          // creating a transaction in the table service
          // this transaction is not yet active and has no id
          TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

          // do something else in the transaction
          transaction.executeDataQuery("SELECT 1").join();
          // now the transaction is active and has an id
          // analyzeQueryResultIfNeeded();

          Status updateStatus = reader.updateOffsetsInTransaction(transaction,
                          message.getPartitionOffsets(), new UpdateOffsetsInTransactionSettings.Builder().build())
                  // Do not commit a transaction without waiting for updateOffsetsInTransaction result to avoid a race condition
                  .join();
          if (!updateStatus.isSuccess()) {
              logger.error("Couldn't update offsets in a transaction: {}", updateStatus);
              return; // retry or shutdown
          }

          Status commitStatus = transaction.commit().join();
          analyzeCommitStatus(commitStatus);
      }
    }
    ```

  {% endlist %}

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Full example of reading a topic in a transaction with writing to a table: [`topic-read-in-transaction-example.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/topic-read-in-transaction-example.rs).


  ```rust
  let batch = reader.pop_batch_in_tx(&mut tx).await?;
  // processing batch.messages and committing transaction
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Handling server read interruption {#stop}

In {{ ydb-short-name }}, server-side balancing of partitions between clients is used. This means that the server can interrupt reading messages from arbitrary partitions.

With a *soft interrupt*, the client receives a notification that the server has already finished sending messages from the partition and no more messages will be read. The client can complete message processing and send an acknowledgment to the server.

In case of a *hard interruption*, the client receives a notification that it can no longer work with partition messages. The client must stop processing the read messages. Unacknowledged messages will be passed to another reader.

#### Soft read interrupt {#soft-stop}

{% list tabs group=lang %}

- C++

  A soft interrupt comes as an `TStopPartitionSessionEvent` event with the `Confirm` method. The client can finish processing messages and send an acknowledgment to the server.

  A fragment of the event loop might look like this:


  ```cpp
  auto event = readSession->GetEvent(/*block=*/true);
  if (auto* stopPartitionSessionEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
      stopPartitionSessionEvent->Confirm();
  } else {
    // other event types
  }
  ```

- Go

  The client code immediately receives all messages available in the buffer (on the SDK side), even if there are not enough to form a packet during batch processing.


  ```go
  r, _ := db.Topic().StartReader("my-consumer", nil,
      topicoptions.WithBatchReadMinCount(1000),
  )

  for {
      batch, _ := r.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
      processBatch(batch)
      _ = r.Commit(batch.Context(), batch)
  }
  ```

- Python

  No special processing is required.

  {% list tabs %}

  - Native SDK

    ```python
    while True:
        batch = reader.receive_batch()
        process(batch)
        reader.commit(batch)
    ```

  - Native SDK (Asyncio)

    ```python
    while True:
        batch = await reader.receive_batch()
        process(batch)
        reader.commit(batch)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Not relevant, because the synchronous reader does not allow configuring handling of such events.
    The client will immediately respond to the server with a stop confirmation.

  - Asynchronous API

    To be able to respond to such an event, override the `onStopPartitionSession(StopPartitionSessionEvent event)` method in the `ReadEventHandler` descendant object (see [Connecting to a topic for reading messages](#start-reader)).
    `event.confirm()` must be called because the server expects this response to continue the shutdown.


    ```java
    @Override
    public void onStopPartitionSession(StopPartitionSessionEvent event) {
      logger.info("Partition session {} stopped. Committed offset: {}", event.getPartitionSessionId(),
              event.getCommittedOffset());
      // This event means that no more messages will be received by server
      // Received messages still can be read from ReaderBuffer
      // Messages still can be committed, until confirm() method is called

      // Confirm that session can be closed
      event.confirm();
    }
    ```

  {% endlist %}
- C#

  No special processing is required.
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  Rust SDK handles stop and close partition session events internally; a public API for configuring soft or hard read interruption is not yet available.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Hard read interrupt {#hard-stop}

{% list tabs group=lang %}

- C++

  A hard interrupt comes as an `TPartitionSessionClosedEvent` event either in response to acknowledgment of a soft interrupt or when the connection to the partition is lost. You can find out the reason by calling the `GetReason` method.

  An event loop fragment might look like this:


  ```cpp
  auto event = readSession->GetEvent(/*block=*/true);
  if (auto* partitionSessionClosedEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
      if (partitionSessionClosedEvent->GetReason() == NYdb::NTopic::TPartitionSessionClosedEvent::EReason::ConnectionLost) {
          std::cout << "Connection with partition was lost" << std::endl;
      }
  } else {
    // other event types
  }
  ```

- Go

  When reading is interrupted, the context of the message or message batch will be canceled.


  ```go
  ctx := batch.Context() // batch.Context() will cancel if partition revoke by server or connection broke
  if len(batch.Messages) == 0 {
      return
  }

  buf := &bytes.Buffer{}
  for _, mess := range batch.Messages {
      buf.Reset()
      _, _ = buf.ReadFrom(mess)
      _, _ = io.Copy(buf, mess)
      writeMessagesToDB(ctx, buf.Bytes())
  }
  ```

- Python

  In this example, message processing in a batch will stop if a partition is revoked during operation. Such optimization requires additional code on the client side. In simple cases, when processing revoked partitions is not a problem, it can be omitted.

  {% list tabs %}

  - Native SDK

    ```python
    def process_batch(batch):
        for message in batch.messages:
            if not batch.alive:
                return False
            process(message)
        return True

    batch = reader.receive_batch()
    if process_batch(batch):
        reader.commit(batch)
    ```

  - Native SDK (Asyncio)

    ```python
    def process_batch(batch):
        for message in batch.messages:
            if not batch.alive:
                return False
            process(message)
        return True

    batch = await reader.receive_batch()
    if process_batch(batch):
        reader.commit(batch)
    ```

  {% endlist %}
- Java

  {% list tabs %}

  - Synchronous API

    Not applicable, since in the synchronous reader there is no way to configure handling of such events.

  - Asynchronous API

    ```java
    @Override
    public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
      logger.info("Partition session {} is closed.", event.getPartitionSession().getPartitionId());
    }
    ```

  {% endlist %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  The Rust SDK handles stop and close events of a partition session internally; there is no public API for configuring soft or hard read interruption yet.

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Support for topic auto-scaling {#autoscaling}

{% list tabs group=lang %}

- C++

  The SDK supports two topic reading modes with auto-scaling enabled: full support mode and compatibility mode. The reading mode is set in the parameters of creating a reading session. By default, compatibility mode is used.


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .SetAutoscalingSupport(true); // full support is enabled

  // or

  auto settings = NYdb::NTopic::TReadSessionSettings()
      .SetAutoscalingSupport(false); // compatibility mode is enabled

  auto readSession = topicClient.CreateReadSession(settings);
  ```


  In full support mode, when all messages from a partition have been read, the `TEndPartitionSessionEvent` event arrives. After receiving this event, no new messages for reading will appear in the partition. To continue reading from child partitions, you must call `Confirm()`, thereby confirming that the application is ready to receive messages from child partitions. If messages from all partitions are processed in a single thread, then `Confirm()` can be called immediately after receiving `TEndPartitionSessionEvent`. If message processing from different partitions is performed in different threads, then you should finish processing the messages, for example, execute the accumulated batch, commit them, or save the read position in your own database, and only then call `Confirm()`.

  After receiving `TEndPartitionSessionEvent` and processing all messages, it is recommended to always commit them immediately. This will balance reading of child partitions among different reading sessions, leading to even load distribution across all readers.

  A fragment of the event loop might look like this:


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .SetAutoscalingSupport(true);

  auto readSession = topicClient.CreateReadSession(settings);

  auto event = readSession->GetEvent(/*block=*/true);
  if (auto* endPartitionSessionEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&*event)) {
      endPartitionSessionEvent->Confirm();
  } else {
    // other event types
  }
  ```


  In compatibility mode, there is no explicit signal that reading from a partition is complete, and the server will try to heuristically determine that the client has processed the partition to the end. This may cause a delay between finishing reading from the original partition and starting reading from its child partitions.

  If the client commits messages, then the signal that message processing from a partition is complete will be the commit of the last message of that partition. If the client does not commit messages, the server will periodically interrupt reading from the partition and switch to reading in another session (if there are other sessions ready to process the partition). This will continue until reading [starts](#client-commit) from the end of the partition.

  It is recommended to verify the correctness of handling a soft read interrupt: the client must process the received messages, commit them, or save the read position in its own database, and only then call `Confirm()` for the `TStopPartitionSessionEvent` event.
- Go

  Enabling topic autoscaling during its creation is done using the `topicoptions.CreateWithAutoPartitioningSettings` option:


  ```go
  import (
    ...

    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
  )

  err := db.Topic().Create(ctx,
    "topic",
    topicoptions.CreateWithAutoPartitioningSettings(
      topictypes.AutoPartitioningSettings{
        AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
      },
    ),
  )
  ```


  If necessary, you can set other parameters in AutoPartitioningSettings:


  ```go
  err := db.Topic().Create(ctx,
    "topic",
    topicoptions.CreateWithAutoPartitioningSettings(
      topictypes.AutoPartitioningSettings{
        AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
        AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
          StabilizationWindow:    time.Minute,
          UpUtilizationPercent:   80,
        },
      },
    ),
  )
  ```


  Enabling autoscaling for an existing topic is done using the `topicoptions.AlterWithAutoPartitioningStrategy` option of `.Topic().Alter`:


  ```go
  import (
    ...

    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
  )

  err := db.Topic().Alter(
    ctx,
    "topic",
    topicoptions.AlterWithAutoPartitioningStrategy(
      topictypes.AutoPartitioningStrategyScaleUp,
    ),
  )

  // other options
  err := db.Topic().Alter(
    ctx,
    "topic",
    topicoptions.AlterWithAutoPartitioningStrategy(
      topictypes.AutoPartitioningStrategyScaleUp,
    ),
    topicoptions.AlterWithAutoPartitioningWriteSpeedStabilizationWindow(time.Minute),
    topicoptions.AlterWithAutoPartitioningWriteSpeedUpUtilizationPercent(80),
  )
  ```


  The SDK supports two reading modes for topics with autoscaling enabled: full support mode and compatibility mode. The reading mode is set by the `topicoptions.WithReaderSupportSplitMergePartitions` option when creating a reader. By default, full support mode (`true`) is used.


  ```go
  import (
    ...

    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
  )

  // full support mode (auto-scaling handling in SDK, default)
  reader, err := db.Topic().StartReader(
    "consumer",
    topicoptions.ReadTopic("topic"),
    topicoptions.WithReaderSupportSplitMergePartitions(true),
  )

  // compatibility mode (auto-scaling handling on the server)
  reader, err := db.Topic().StartReader(
    "consumer",
    topicoptions.ReadTopic("topic"),
    topicoptions.WithReaderSupportSplitMergePartitions(false),
  )
  ```

- Python

  Enabling topic autoscaling during its creation is done using the `auto_partitioning_settings` argument of `create_topic`:

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.create_topic(
        topic,
        consumers=[consumer],
        min_active_partitions=10,
        max_active_partitions=100,
        auto_partitioning_settings=ydb.TopicAutoPartitioningSettings(
            strategy=ydb.TopicAutoPartitioningStrategy.SCALE_UP,
            up_utilization_percent=80,
            down_utilization_percent=20,
            stabilization_window=datetime.timedelta(seconds=300),
        ),
    )
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.create_topic(
        topic,
        consumers=[consumer],
        min_active_partitions=10,
        max_active_partitions=100,
        auto_partitioning_settings=ydb.TopicAutoPartitioningSettings(
            strategy=ydb.TopicAutoPartitioningStrategy.SCALE_UP,
            up_utilization_percent=80,
            down_utilization_percent=20,
            stabilization_window=datetime.timedelta(seconds=300),
        ),
    )
    ```

  {% endlist %}

  Making changes to an existing topic is done using the `alter_auto_partitioning_settings` argument of `alter_topic`:


  ```python
      driver.topic_client.alter_topic(
          topic_path,
          alter_auto_partitioning_settings=ydb.TopicAlterAutoPartitioningSettings(
              set_strategy=ydb.TopicAutoPartitioningStrategy.SCALE_UP,
              set_up_utilization_percent=80,
              set_down_utilization_percent=20,
              set_stabilization_window=datetime.timedelta(seconds=300),
          ),
      )
  ```


  The SDK supports two reading modes for topics with autoscaling enabled: full support mode and compatibility mode. The reading mode is set by the `auto_partitioning_support` argument when creating a reader. By default, full support mode is used.


  ```python
  reader = driver.topic_client.reader(
      topic,
      consumer,
      auto_partitioning_support=True, # Full support is enabled
  )

  # or

  reader = driver.topic_client.reader(
      topic,
      consumer,
      auto_partitioning_support=False, # Compatibility mode is enabled
  )
  ```


  From a practical standpoint, the modes do not differ for the end user. Full support mode differs from compatibility mode in who guarantees the reading order — the client or the server. Compatibility mode is achieved through server-side processing and generally works slower.
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

### Commit outside the reader {#commit-outside-the-reader}

Most often, it is convenient to commit within the reader that receives messages. However, there are scenarios where the commit must be performed by a process different from the reading process. In such a case, a commit method located outside the reader is needed.

{% list tabs group=lang %}

- C++

  Commit outside the reading session is performed using the `NYdb::NTopic::TTopicClient::CommitOffset` method:


  ```cpp
  #include <ydb-cpp-sdk/client/topic/client.h>

  NYdb::NTopic::TTopicClient topicClient(driver);

  NYdb::NStatusHelpers::ThrowOnError(topicClient.CommitOffset(
      topicPath,
      partitionId,
      consumerName,
      offset).GetValueSync());
  ```


  If an active read session exists at the time of confirmation (for example, via `CreateReadSession`), it is recommended to pass its ID using the `ReadSessionId` option in `NYdb::NTopic::TCommitOffsetSettings`. This allows the server not to interrupt the current read session:


  ```cpp
  // Getting the read session identifier
  std::string sessionId = readSession->GetSessionId();

  NYdb::NStatusHelpers::ThrowOnError(topicClient.CommitOffset(
      topicPath,
      partitionId,
      consumerName,
      offset,
      NYdb::NTopic::TCommitOffsetSettings()
          .ReadSessionId(sessionId)
  ).GetValueSync());
  ```

- Go

  Acknowledgment of processing outside the reader is performed using the `db.Topic().CommitOffset` method:


  ```go
  // Basic method — offset acknowledgment without an active read session
  err := db.Topic().CommitOffset(
    ctx,
    topicPath,
    partitionID,
    consumer,
    offset,
  )
  ```


  If an active read session exists at the time of confirmation (via `StartReader` or `StartListener`), it is recommended to pass its ID using the `WithCommitOffsetReadSessionID` option. This allows the server not to interrupt the current read session:


  ```go
  import (
    // ...
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
  )

  // Getting the read session identifier
  sessionID := reader.ReadSessionID()
  // or: sessionID := listener.ReadSessionID()

  err = db.Topic().CommitOffset(
    ctx,
    topicPath,
    partitionID,
    consumer,
    offset,
    topicoptions.WithCommitOffsetReadSessionID(sessionID),
  )
  ```

- Python

  Acknowledgment of processing outside the reader is performed using the `topic_client.commit_offset` method:

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.commit_offset(
        topic_path,
        consumer_name,
        partition_id,
        offset,
        reader.read_session_id,  # опционально: не прерывает активную сессию чтения
    )
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.commit_offset(
        topic_path,
        consumer_name,
        partition_id,
        offset,
        reader.read_session_id,  # опционально: не прерывает активную сессию чтения
    )
    ```

  {% endlist %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java


  ```java
  TopicClient client = ...;

  String sessionID = reader.getSessionId();
  // For AsyncReader, the session identifier can be obtained when processing the SessionStartedEvent

  client.commitOffset(
      topicPath,
      CommitOffsetSettings.newBuilder()
          .setReadSessionId(sessionID)
          .setPartitionId(partitionID)
          .setConsumer(consumer)
          .setOffset(offset)
          .build()
  ).join().expectSuccess("Error commit!");
  ```

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#330](https://github.com/ydb-platform/ydb-rs-sdk/issues/330)
- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
