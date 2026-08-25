# Working with topics

This article provides examples of how to use the {{ ydb-short-name }} SDK to work with [topics](../../concepts/datamodel/topic.md).

Before performing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Topic usage examples

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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Initializing a connection to topics

{% list tabs group=lang %}

- Go

  Use a {{ ydb-short-name }} driver instance created with `ydb.Open`. The topic client is available via `db.Topic()`.


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

- C++

  To work with topics, instances of the {{ ydb-short-name }} driver and client are created.

  The {{ ydb-short-name }} driver lets the app and {{ ydb-short-name }} interact at the transport layer. The driver must exist during the YDB access lifecycle and be initialized before creating a client.

  Topic client ([source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) requires the {{ ydb-short-name }} driver for work. It handles topics and manages read and write sessions.

  Application code snippet for initializing the {{ ydb-short-name }} driver:


  ```cpp
  // Create driver instance.
  auto driverConfig = NYdb::TDriverConfig()
      .SetEndpoint(opts.Endpoint)
      .SetDatabase(opts.Database)
      .SetAuthToken(std::getenv("YDB_TOKEN"));

  NYdb::TDriver driver(driverConfig);
  ```


  This example uses authentication token from the `YDB_TOKEN` environment variable. For details see [Connecting to a database](../../concepts/connect.md) and [Authentication](../../security/authentication.md) pages.

  App code snippet for creating a client:


  ```cpp
  NYdb::NTopic::TTopicClient topicClient(driver);
  ```

- Java

  To work with topics, instances of the {{ ydb-short-name }} transport and client are created.

  To interact with {{ ydb-short-name }} Topics, create an instance of the {{ ydb-short-name }} transport and topic client.

  Application code snippet for initializing the {{ ydb-short-name }} transport:


  ```java
  try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
          .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
          .build()) {
      // Use YDB transport
  }
  ```


  In this example `CloudAuthHelper.getAuthProviderFromEnviron()` helper method is used which retrieves auth token from environment variables.
  For example, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  For details see [Connecting to a database](../../concepts/connect.md) and [Authentication](../../security/authentication.md) pages.

  Topic client ([source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) uses {{ ydb-short-name }} transport and handles all topics topic operations, manages read and write sessions.

  App code snippet for creating a client:


  ```java
  try (TopicClient topicClient = TopicClient.newClient(transport)
                .setCompressionExecutor(compressionExecutor)
                .build()) {
    // Use topic client
  }
  ```


  Both provided examples use ([try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)) block.
  It allows to automatically close  client and transport on leaving this block, considering both classes extends `AutoCloseable`.

- C#

  To work with topics, you need to create an instance of the {{ ydb-short-name }} driver.

  To interact with {{ ydb-short-name }} Topics, create an instance of the {{ ydb-short-name }} driver and topic client.

  Application code snippet for initializing the {{ ydb-short-name }} driver:


  ```c#
  var config = new DriverConfig(
      endpoint: "grpc://localhost:2136",
      database: "/local"
  );

  await using var driver = await Driver.CreateInitialized(
      config: config,
      loggerFactory: loggerFactory
  );
  ```


  This example uses anonymous authentication. For details, see [Connecting to a database](../../concepts/connect.md) and [Authentication](../../security/authentication.md).

  App code snippet for creating various clients:


  ```c#
  var topicClient = new TopicClient(driver);

  await using var writer = new WriterBuilder<string>(driver, topicName)
  {
      ProducerId = "ProducerId_Example"
  }.Build();

  await using var reader = new ReaderBuilder<string>(driver)
  {
      ConsumerName = "Consumer_Example",
      SubscribeSettings = { new SubscribeSettings(topicName) }
  }.Build();
  ```

- Python

  To work with topics, create a {{ ydb-short-name }} driver instance. The topic client is available via the `topic_client` attribute and is used for management operations on topics and for creating writers and readers.

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

  For more on [connecting to the database](../../concepts/connect.md) and [authentication](../../security/authentication.md).

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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Managing topics {#manage}

### Creating a topic {#create-topic}

The topic path is mandatory. Other parameters are optional.

{% list tabs group=lang %}

- C++

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L394).

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

  For a full list of supported parameters, see the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions:


  ```go
  err := db.Topic().Create(ctx, "topic-path",
    // optional
    topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

    // optional
    topicoptions.CreateWithMinActivePartitions(3),
  )
  ```

- Python

  Example of creating a topic with a list of supported codecs and a minimum number of partitions:

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

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

  Example of creating a topic with a list of supported codecs and a minimum number of partitions:


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

  When you update a topic, you must specify the topic path and the parameters to be changed.


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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Modifying a topic {#alter-topic}

{% list tabs group=lang %}

- C++

  When modifying a topic, in the parameters of the `AlterTopic` method, specify the topic path and the parameters to be changed. The parameters to be changed are represented by the `TAlterTopicSettings` structure.

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  Example of adding an [important consumer](../../concepts/datamodel/topic#important-consumer) and setting two days [retention time](../../concepts/datamodel/topic#retention-time) for the topic:


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

  When modifying a topic, specify the topic path and the parameters to be changed.

  For a full list of supported parameters, see the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

  Example of adding a consumer to a topic:


  ```go
  err := db.Topic().Alter(ctx, "topic-path",
    topicoptions.AlterWithAddConsumers(topictypes.Consumer{
      Name:            "new-consumer",
      SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
    }),
  )
  ```

- Python

  Example of updating a topic's list of supported codecs and minimum number of partitions:

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

  When modifying a topic, in the parameters of the `alterTopic` method, specify the topic path and the parameters to be changed.

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/AlterTopicSettings.java#L23).


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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Getting topic information {#describe-topic}

{% list tabs group=lang %}

- C++

  To get information about a topic, use the `DescribeTopic` method.

  Use `TTopicDescription` method to get information about topic.

  For a full list of description fields, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L163).

  Example of using topic description:


  ```cpp
  auto result = topicClient.DescribeTopic("my-topic").GetValueSync();
  if (result.IsSuccess()) {
      const auto& description = result.GetTopicDescription();
      std::cout << "Topic description: " << GetProto(description) << std::endl;
  }
  ```


  There is another method `DescribeConsumer` to get informtaion about consumer.

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

  Use `describeTopic` method to get information about topic.

  For a full list of description fields, see the [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).


  ```java
  Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
          .join();
  TopicDescription description = topicDescriptionResult.getValue();
  ```

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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Deleting a topic {#drop-topic}

To delete a topic, just specify the path to it.

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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Message writes {#write}

### Connecting to a topic for message writes {#start-writer}

Only connections with matching [producer and message group](../../concepts/datamodel/topic#producer-id) identifiers are currently supported (`producer_id` shoud be equal to `message_group_id`). This restriction will be removed in the future.

{% list tabs group=lang %}

- C++

  A write connection to a topic is represented by a write session object with the `IWriteSession` or `ISimpleBlockingWriteSession` interface (a variant for simple single-message writes without acknowledgment, blocking when the number of inflight writes or the SDK buffer size is exceeded). Write session settings are represented by the `TWriteSessionSettings` structure; for the `ISimpleBlockingWriteSession` variant, some settings are not supported.

  For a full list of write session settings, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1199).

  Example of creating a write session with the `IWriteSession` interface.


  ```cpp
  std::string producerAndGroupID = "group-id";
  auto settings = NYdb::NTopic::TWriteSessionSettings()
      .Path("my-topic")
      .ProducerId(producerAndGroupID)
      .MessageGroupId(producerAndGroupID);

  auto session = topicClient.CreateWriteSession(settings);
  ```

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

  - Java (sync)

    Writer settings initialization:


    ```java
    String producerAndGroupID = "group-id";
    WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .build();
    ```


    Sync writer creation:


    ```java
    SyncWriter writer = topicClient.createSyncWriter(settings);
    ```


    Writer should be initialized after it is created. There are two methods to do that:

    - `init()`: non-blocking, launches initialization in background and doesn't wait for it to finish.


      ```java
      writer.init();
      ```

    - `initAndWait()`: blocking, launches initialization and waits for it to finish.
      If an error occurs during this process, exception will be thrown.


      ```java
      try {
          writer.initAndWait();
          logger.info("Init finished successfully");
      } catch (Exception exception) {
          logger.error("Exception while initializing writer: ", exception);
          return;
      }
      ```

  - Java (async)

    Writer settings initialization:


    ```java
    String producerAndGroupID = "group-id";
    WriterSettings settings = WriterSettings.newBuilder()
          .setTopicPath(topicPath)
          .setProducerId(producerAndGroupID)
          .setMessageGroupId(producerAndGroupID)
          .build();
    ```


    Async writer creation and initialization:


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
  await using var writer = new WriterBuilder<string>(driver, topicName)
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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages {#writing-messages}

{% list tabs group=lang %}

- C++

  `IWriteSession` interface allows asynchronous write.

  User interaction with the `IWriteSession` object is generally structured as event loop processing with three event types: `TReadyToAcceptEvent`, `TAcksEvent`, and `TSessionClosedEvent`.

  For each kind of event user can set a handler in write session settings before session creation. Also, a common handler can be set.

  If handler is not set for a particular event, it will be delivered to SDK client via `GetEvent` / `GetEvents` methods. `WaitEvent` method allows user to await for a next event in non-blocking way with `TFuture<void>()` interface.

  To write a message, user uses a move-only `TContinuationToken` object, which has been created by the SDK and has been delivered to the user with a `TReadyToAcceptEvent` event. During write user can set an arbitrary sequential number and a message creation timestamp. By default they are generated by the SDK.

  `Write` is asynchronous. Data from messages is processed and stored in the internal buffer. Settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, and `BatchFlushSizeBytes` control sending in the background. Write session reconnects to the {{ ydb-short-name }} if the connection fails and resends the message if possible, with regard to `RetryPolicy` setting. If an error that cannot be repeated is received, write session stops and sends `TSessionClosedEvent` to the client.

  Example of writing using event loop without any handlers set up:


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

- Go

  To send a message, just save Reader in the Data field, from which the data can be read. You can expect the data of each message to be read once (or until the first error). By the time you return the data from Write, it will already have been read and stored in the internal buffer.

  By default, SeqNo and the message creation date are set automatically.

  By default, Write is performed asynchronously: data from messages is processed and stored in the internal buffer, sending is done in the background. Writer reconnects to the {{ ydb-short-name }} if the connection fails and resends the message if possible. If an error that cannot be repeated is received , Writer stops and subsequent Write calls will end with an error.


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

- Python

  To deliver messages, you can either simply transmit message content (bytes, str) or set certain properties manually. You can send objects one-by-one or as a list. The `write` method is asynchronous. The method returns immediately once messages are put to the client's internal buffer; this is usually a fast process. If the internal buffer is filled up, you might need to wait until part of the data is sent to the server.

  {% list tabs %}

  - Native SDK

    ```python
    # Simple message sending, without explicitly specifying metadata.
    # Convenient to start with, convenient to use while only the message content matters.
    writer = driver.topic_client.writer(topic_path)
    writer.write("mess")  # Strings will be sent in utf-8 encoding, convenient for sending
                          # text messages.
    writer.write(bytes([1, 2, 3]))  # These bytes will be sent "as is", convenient for sending
                                    # binary data.
    writer.write(["mess-1", "mess-2"])  # Here, multiple messages are sent in one call —
                                         # reducing overhead on internal SDK processes,
                                         # makes sense for a large message flow.

    # Full form, used when, in addition to the message content, you need to manually set its properties.
    writer = driver.topic_client.writer(topic="topic-path", auto_seqno=False, auto_created_at=False)

    writer.write(ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()))
    writer.write(ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now()))

    # In the full form, you can also send multiple messages in one function call.
    # This makes sense for a large flow of sent messages — to reduce
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

  - Java (sync)

    Method `send` blocks until a message is put into writers sending queue.
    Putting a message into this queue means that the writer will do its best to deliver it.
    For example, if a writing session will be accidentally closed, the writer will reconnect and try to resend this message on a new session.
    But putting a message into message queue has no guarantees that this message will be written.
    For example, there could be errors that will lead to writer shutdown before messages from the queue are sent.
    If you have to be sure for each message that it is written, use async writer and check status returned by `send` method.


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

  - Java (async)

    The `send` method in the asynchronous client is non-blocking. It places the message into the send queue.
    The method returns `CompletableFuture<WriteAck>`, which allows you to check whether the message was actually written.
    If the queue is full, a QueueOverflowException is thrown.
    This is a way to signal the user that the write flow should be slowed down.
    In such a case, you should either skip messages or retry writing with exponential backoff.
    You can also increase the client buffer size (`setMaxSendBufferMemorySize`) to handle a larger volume of messages before it fills up.


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

  // To send immediately, call flush
  await writer.flush();

  // Or close the writer
  await writer.close();
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Message writes with storage confirmation on the server

{% list tabs group=lang %}

- C++

  `IWriteSession` interface allows getting server acknowledgments for writes.

  Status of server-side message write is represented with `TAcksEvent`. One event can contain the statuses of several previously sent messages.Status is one of the following: message write is confirmed (`EES_WRITTEN`), message is discarded as a duplicate of a previously written message (`EES_ALREADY_WRITTEN`) or message is discarded because of failure (`EES_DISCARDED`).

  Example of setting TAcksEvent handler for a write session:


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


  In this write session user does not receive `TAcksEvent` events in the `GetEvent` / `GetEvents` loop. Instead, SDK will call given handler on every acknowledgment coming from server. In the same way user can set up handlers for other types of events.

- Go

  When connected, you can specify the synchronous message write option: topicoptions.WithSyncWrite(true). Then Write will only return after receiving a confirmation from the server that all messages passed in the call have been saved. If necessary, the SDK will reconnect and retry sending messages as usual. In this mode, the context only controls the response time from the SDK, meaning the SDK will continue trying to send messages even after the context is canceled.


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

  There are two ways to get a message write acknowledgement from the server:

  - `flush()`: Sends a message and waits for the acknowledgement of its delivery from the server. This method is slow when you are sending multiple messages in a row.
  - Blocking method `write_with_ack(...)` waits until all the messages previously written to the internal buffer are acknowledged:

  {% list tabs %}

  - Native SDK

    ```python
    # Put several messages into the internal buffer, then wait,
    # until all of them are delivered to the server.
    for mess in messages:
        writer.write(mess)

    writer.flush()

    # You can send multiple messages and wait for acknowledgment for the entire group.
    writer.write_with_ack(["mess-1", "mess-2"])

    # Waiting when sending each message — this method will return a result only after receiving
    # confirmation from the server.
    # This is the slowest way to send messages, use it only if such a mode
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

  `send` method returns `CompletableFuture<WriteAck>`.
  Its successful completion means that the fact that this message is written is confirmed by server.
  `WriteAck` struct contains seqNo, offset and write status:


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

  Asynchronous writing of a message to a topic. If the internal buffer overflows, it waits for the buffer to be released before resending.


  ```c#
  await writer.WriteAsync("Hello, Example YDB Topics!");
  ```


  If the server is unavailable, messages may accumulate in the queue waiting to be sent. To control the waiting time, you can use a cancellation token (`CancellationToken`). However, with this approach, there is a risk that the user may cancel the sending of an already acknowledged message.


  ```c#
  var writeCts = new CancellationTokenSource();
  writeCts.CancelAfter(TimeSpan.FromSeconds(3));

  await writer.WriteAsync("Hello, Example YDB Topics!", writeCts.Token);
  ```

- JavaScript

  All messages are written to an internal buffer. There are 3 mechanisms for sending to the server: two automatic and one manual. The manual one is calling the `writer.flush` method, which returns the last seqno written on the server. Automatic sending occurs under the following conditions:

  - Exceeding the internal buffer size `maxBufferBytes` (default value = 256MiB).
  - If the server is unavailable, messages may accumulate while waiting to be sent. In this case, you can pass a cancellation token (`flushIntervalMs`) to control waiting. However, if the user cancels the recorded message, it will still be canceled.


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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Selecting a codec for message compression {#codec}

For more details on using data compression for topics, see [here](../../concepts/datamodel/topic#message-codec).

{% list tabs group=lang %}

- C++

  The compression used when sending messages with the `Write` method is set when [creating a write session](#start-writer) with the `Codec` and `CompressionLevel` settings. By default, the GZIP codec is selected.
  Example of creating a write session without message compression:


  ```cpp
  auto settings = NYdb::NTopic::TWriteSessionSettings()
    // other settings are set here
    .Codec(ECodec::RAW);

  auto session = topicClient.CreateWriteSession(settings);
  ```


  Write session allows sending a message compressed with other codec. For this use `WriteEncoded` method, specify codec used and original message byte size. The codec must be allowed in topic settings.

- Go

  If necessary, a fixed codec can be set in the connection options. It will then be used and no measurements will be taken.

  By default, the SDK selects the codec automatically based on topic settings. In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then it sometimes tries to compress messages with all the available codecs, and then selects the codec that yields the smallest message size. If the list of allowed codecs for the topic is empty, the SDK makes automatic selection between Raw and Gzip codecs.


  ```go
  producerAndGroupID := "group-id"
  writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
    topicoptions.WithMessageGroupID(producerAndGroupID),
    topicoptions.WithCodec(topictypes.CodecGzip),
  )
  ```

- Python

  If necessary, a fixed codec can be set in the connection options. It will then be used and no measurements will be taken.

  If no ProducerId is specified on write session setup, the session runs in no-deduplication mode. The example below deminstrates such a session setup:


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
    codec: 10000, // CUSTOM (valid range: 10000–19999)
  });
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Writing messages without deduplication {#nodedup}

For more information about writing without deduplication, see the [corresponding concepts section](../../concepts/datamodel/topic#no-dedup).

{% list tabs group=lang %}

- C++

  If the `ProducerId` option is not specified in the write session settings, a write session without deduplication will be created.
  Example of creating such a write session:


  ```cpp
  auto settings = NYdb::NTopic::TWriteSessionSettings()
      .Path(myTopicPath);

  auto session = topicClient.CreateWriteSession(settings);
  ```


  To enable deduplication, specify the `ProducerId` option in the write session settings or explicitly enable deduplication by calling the `DeduplicationEnabled()` method, for example, as in the ["Connecting to a topic"](#start-writer) section.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Go

  In **ydb-go-sdk**, when you create a writer without explicitly passing `topicoptions.WithWriterProducerID`, the SDK still assigns a producer ID (it generates one automatically). A mode equivalent to omitting `ProducerId` in the C++ example above is not available in the current SDK version.

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Using message metadata feature {#messagemeta}

You can provide some metadata for any particular message when writing. This metadata can be a list of up to 100 key-value pairs per message.
All the metadata provided when writing a message is sent to a consumer with the message during reading.

{% list tabs group=lang %}

- C++

  To take advantage of message metadata feature, use the `Write()` method with `TWriteMessage`argument as below:


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


  Or each `Metadata` can be added individually:


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

  When constructing a message for writing using a Builder, you can pass it objects of type `MetadataItem` with a key of type `String` + value of type `byte[]`.

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

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Write in a transaction {#write-tx}

{% list tabs group=lang %}

- C++

  To write to a topic within a transaction, it is necessary to pass a transaction object reference to the `Write` method of the writing session.

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

- Go

  [Example on GitHub](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic#Client.StartTransactionalWriter)

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

  To write to a topic in a transaction, you need to create a transactional writer by calling `topic_client.tx_writer`. After that, you can send messages as usual. You don't need to close the transactional writer — it happens automatically when the transaction completes.

  In the example below, there is no explicit call to `tx.commit()`; it occurs implicitly upon the successful execution of the `callee` lambda.

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

  - Java (sync)

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteSync.java)

    Transaction can be set in the `SendSettings` argument of the `send` method while sending a message.
    Such a message will be written on the transaction commit.


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

  - Java (async)

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionWriteAsync.java)

    Transaction can be set in the `SendSettings` argument of the `send` method while sending a message.
    Such a message will be written on the transaction commit.


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

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Reading messages {#reading}

### Connecting to a topic for message reads {#start-reader}

Reading messages from a topic can be done by specifying a Consumer associated with that topic, as well as without a Consumer. If a Consumer is not specified, the client application must calculate the offset for reading messages on its own. A more detailed example of reading without a Consumer is discussed in the [relevant section](#no-consumer).

A Consumer can be created on [creating](#create-topic) or [altering](#alter-topic) a topic.
Topic can have several Consumers and for each of them server stores its own reading progress.

{% list tabs group=lang %}

- C++

  A connection for reading from one or more topics is represented by a read session object with the `IReadSession` interface. The read session settings are represented by the `TReadSessionSettings` structure.

  See the full list of settings [in the header file](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  To establish a connection to the existing `my-topic` topic using the added `my-consumer` consumer, use the following code:


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic");

  auto session = topicClient.CreateReadSession(settings);
  ```

- Go

  To establish a connection to the existing `my-topic` topic using the added `my-consumer` consumer, use the following code:


  ```go
  reader, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"))
  if err != nil {
      return err
  }
  ```

- Python

  To establish a connection to the existing `my-topic` topic using the added `my-consumer` consumer, use the following code:

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

  - Java (sync)

    Reader settings initialization:


    ```java
    ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)  // name of the consumer registered on the topic
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // read from this timestamp (optional)
                  .setMaxLag(Duration.ofMinutes(30)) // maximum lag from the end of the queue (optional)
                  .build())
          .build();
    ```


    Sync reader creation:


    ```java
    SyncReader reader = topicClient.createSyncReader(settings);
    ```


    After a reader is created, it has to be initialized. Sync reader has two methods for this:

    - `init()`: non-blocking, launches initialization in background and does not wait for it to finish.


    ```java
    reader.init();
    ```


    - `initAndWait()`: blocking, launches initialization and waits for it to finish.
      If an error occurs during this process, exception will be thrown.


    ```java
    try {
        reader.initAndWait();
        logger.info("Init finished successfully");
    } catch (Exception exception) {
        logger.error("Exception while initializing reader: ", exception);
        return;
    }
    ```

  - Java (async)

    Reader settings initialization:


    ```java
    ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)  // name of the consumer registered on the topic
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // read from this timestamp (optional)
                  .setMaxLag(Duration.ofMinutes(30)) // maximum lag from the end of the queue (optional)
                  .build())
          .build();
    ```


    For an asynchronous reader, in addition to the general read settings `ReaderSettings`, you will need event handler settings `ReadEventHandlersSettings`, in which you need to pass an instance of a `ReadEventHandler` subclass.
    It will describe how various events that occur during reading should be handled.


    ```java
    ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
          .setEventHandler(new Handler())
          .build();
    ```


    Optionally, in `ReadEventHandlersSettings` you can specify an executor on which message processing will occur; by default, the SDK's internal thread is used.

    To implement an event handler, you can inherit from `AbstractReadEventHandler` and override the `onMessages` method.
    The `onMessages` method is called every time the SDK receives the next batch of messages from the server. Within a single call, one or more messages arrive, which can be acknowledged (`commit`) either individually or after processing the entire batch. Example implementation:


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


    Async reader creation and initialization:


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
  await using var reader = new ReaderBuilder<string>(driver)
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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

Additional options are used to specify multiple topics and other parameters.
To establish a connection to the `my-topic` and `my-specific-topic` topics using the `my-consumer` consumer and also set the time to start reading messages, use the following code:

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


  Also, in the example above, the time from which to start reading messages is set.

- Python

  This feature is under development.

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
  await using var reader = new ReaderBuilder<string>(driver)
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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [consumer offset](../../concepts/datamodel/topic.md#consumer-offset). After reading a message, the client should [send a commit to the server](#commit). The consumer offset changes and only uncommitted messages will be read in case of a new connection.

You can read messages without a [commit](#no-commit) as well. In this case, all uncommited messages, including those processed, will be read if there is a new connection.

Information about which messages have already been processed can be [saved on the client side](#client-commit) by sending the starting consumer offset to the server when creating a new connection. This does not change the consumer offset on the server.

Data from topics can be read in the context of [transactions](#read-tx). In this case, the reading offset will only advance when the transaction is committed. On reconnect, all uncommitted messages will be read again.

{% list tabs group=lang %}

- C++

  The user's interaction with the `IReadSession` object is generally structured as an event loop with the following event types: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TEndPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent`, and `TSessionClosedEvent`.

  For each kind of event user can set a handler in read session settings before session creation. Also, a common handler can be set.

  If handler is not set for a particular event, it will be delivered to SDK client via `GetEvent` / `GetEvents` methods. The `WaitEvent` method allows user to await for a next event in non-blocking way with `TFuture<void>()` interface.

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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without a commit {#no-commit}

#### Reading messages one by one

{% list tabs group=lang %}

- C++

  Reading messages one-by-one is not supported in the C++ SDK. Class `TDataReceivedEvent` represents a batch of read messages.

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

  - Java (sync)

    To read messages one at a time without processing confirmation, use the following code:


    ```java
    while(true) {
      Message message = reader.receive();
      process(message);
    }
    ```

  - Java (async)

    Reading messages one-by-one is not supported in async Reader.

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

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Reading message batches

{% list tabs group=lang %}

- C++

  When establishing a read session with the `SimpleDataHandlers` setting, you only need to pass a handler for data messages. The SDK will call this handler for each batch of messages received from the server. Read acknowledgments will not be sent by default.


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


  In this example, after creating a session, the main thread waits for the server to complete the session in the `GetEvent` method. No other event types will be received.

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

  - Java (sync)

    Reading messages in batches is not supported in sync Reader.

  - Java (async)

    To read a batch of messages without acknowledging their processing, use the following code:


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

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with a commit {#commit}

Confirmation of message processing (commit) informs the server that the message from the topic has been processed by the recipient and does not need to be sent anymore. When using acknowledged reading, it is necessary to confirm all received messages without skipping any. Message commits on the server occur after confirming a consecutive interval of messages "without gaps," and the confirmations themselves can be sent in any order.

For example, if messages 1, 2, 3 are received from the server, the program processes them in parallel and sends confirmations in the following order: 1, 3, 2. In this case, message 1 will be committed first, and messages 2 and 3 will be committed only after the server receives confirmation of the processing of message 2.

If a commit fails with an error, the application should log it and continue; it makes no sense to retry the commit. At this point, it is not known if the message was actually confirmed.

#### Reading messages one by one with commits

{% list tabs group=lang %}

- C++

  Reading messages one-by-one is not supported in the C++ SDK. Class `TDataReceivedEvent` represents a batch of read messages.

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


  By default, `Commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, the reader must be closed explicitly using the `Reader.Close()` call.

- Python

  `commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the latest commits before exiting the program, the reader must be closed explicitly.

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

  To confirm message processing, simply call the `commit` method on the message.
  This applies to both synchronous and asynchronous readers.
  In an asynchronous reader, when processing a batch of messages, you can call `commit` either on the entire batch at once or on each message individually.
  This method returns `CompletableFuture<Void>`, whose successful execution means the server has confirmed message processing.
  If a commit error occurs, do not attempt to retry it. The error is most likely caused by the session being closed.
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

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Reading message batches with commits

{% list tabs group=lang %}

- C++

  Similarly to the [example above](#no-commit), when setting up a read session with the `SimpleDataHandlers` setting, you only need to pass a handler for data messages. The SDK will call this handler for each message batch received from the server. Passing the `commitDataAfterProcessing = true` parameter means that the SDK will send read acknowledgments for all messages to the server after the handler completes.


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

  `commit` is a fast call: it saves data to an internal buffer and immediately returns control, while the actual sending happens later. Therefore, to avoid losing the last commits before exiting the program, the reader must be closed explicitly.

- Java

  {% list tabs %}

  - Java (sync)

    Not relevant due to sync reader only reading messages one by one.

  - Java (async)

    In the `onMessages` handler, you can commit the entire batch of messages by calling `commit` on the event.


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

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading with consumer offset storage on the client side {#client-commit}

Instead of committing messages, the client application may track reading progress on its own. In this case, it can provide a handler, which will be called back on each partition read start. This handler may set a starting reading position for this partition.

{% list tabs group=lang %}

- C++

  When processing `TStartPartitionSessionEvent` events, you can set the position from which reading should start in the response to the server.
  To do this, pass the `readOffset` parameter to the `Confirm` method.
  Additionally, you can pass the `commitOffset` parameter, which specifies the position up to which messages should be considered [committed](#commit).

  Setting handler example:


  ```cpp
  settings.EventHandlers_.StartPartitionSessionHandler(
      [](NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
          auto readFromOffset = GetOffsetToReadFrom(event.GetPartitionId());
          event.Confirm(readFromOffset);
      }
  );
  ```


  In the code above,`GetOffsetToReadFrom` is part of the example, not SDK. Use your own method to provide the correct starting offset for a partition with a given partition id.

  Also, `TReadSessionSettings` has a `ReadFromTimestamp` setting for reading only messages newer than the given timestamp. This setting is intended to skip some messages, not for precise reading start positioning. Several first-received messages may still have timestamps less than the specified one.

- Go

  {% note tip %}

  In the default reader mode, offsets up to the position specified via `res.StartFrom` are committed on the server. After that, re-reading the same messages by shifting the position back becomes impossible. To disable automatic commit, use the no-commit mode when creating the reader.


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

  This feature is under development.

- Java

  Reading from a given offset in Java is only possible in the asynchronous reader. In the event handler `StartPartitionSessionEvent`, when responding to the server, you can set the position from which to start reading. To do this, pass the settings `StartPartitionSessionSettings` with the specified offset via `setReadOffset` to the method `confirm`. Also, by calling `setCommitOffset`, you can specify the offset that should be considered committed.


  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // Long
              .setCommitOffset(lastCommitOffset) // Long
              .build());
  }
  ```


  The `setReadFrom` setting is used for reading only messages with write timestamps no less than the given one.

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

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading without a Consumer {#no-consumer}

Typically, the read progress of a topic is stored on the server in each `Consumer`. However, you can choose not to store such progress on the server and, when creating a reader, explicitly specify that reading will occur without `Consumer`.

{% list tabs group=lang %}

- Go

  Pass an empty string as the consumer name and use the `topicoptions.WithReaderWithoutConsumer(false)` option (this mode is **experimental**; see [VERSIONING](https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md) in the SDK repository). In the read selector, specify the topic path and partition list. Message commits are not available in this mode (`CommitModeNone`); on reconnects you must restore progress on the client side—see [client-side offset storage](#client-commit).


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

  To read without a Consumer, the `withoutConsumer()` method should be called explicitly on the `ReaderSettings` builder:


  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .withoutConsumer()
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(TOPIC_NAME)
                  .build())
          .build();
  ```


  In this case, reading progress on the server will be lost on partition session restart.
  To avoid reading from the beginning each time, starting offsets should be set on each partition session start:


  ```java
  @Override
  public void onStartPartitionSession(StartPartitionSessionEvent event) {
      event.confirm(StartPartitionSessionSettings.newBuilder()
              .setReadOffset(lastReadOffset) // the last offset read by this client, Long
              .build());
  }
  ```

- Python

  To read without a Consumer, create a reader using the `reader` method with the following arguments:

  - `topic` - `ydb.TopicReaderSelector` object with defined `path` and `partitions` list;
  - `consumer` - should be `None`;
  - `event_handler` - inheritor of `ydb.TopicReaderEvents.EventHandler` that implements the `on_partition_get_start_offset` function. This function is responsible for returning the initial offset for reading messages when the reader starts and during reconnections. The client application must specify this offset in the parameter `ydb.TopicReaderEvents.OnPartitionGetStartOffsetResponse.start_offset`. The function can also be implemented as asynchronous.

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

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Reading in a transaction {#read-tx}

{% list tabs group=lang %}

- C++

  Before reading messages, the client code must pass a transaction object reference to the reading session settings.

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

  When processing `events`, you do not need to confirm processing for `TDataReceivedEvent` events explicitly.

  {% endnote %}

  Confirmation of the `TStopPartitionSessionEvent` event processing must be done after calling `Commit`.


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

  To read messages within a transaction, use the [`Reader.PopMessagesBatchTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader#Reader.PopMessagesBatchTx) method. It reads a batch of messages and adds their commit to the transaction; you do not need to commit these messages separately. The message reader can be reused across different transactions. It is important that the order of transaction commits matches the order in which messages are received from the reader, because message commits in a topic must be performed strictly in order. The easiest way to do this is to use the reader in a loop.

  [Example on GitHub](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/topic/topicreader/topic_reader_transaction.go)


  ```go
  for {
    err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
      batch, err := reader.PopMessagesBatchTx(ctx, tx) // the batch will be committed upon the overall transaction commit
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

  To read messages from a topic within a transaction, use the `reader.receive_batch_with_tx` method. It reads a batch of messages and adds their commit to the transaction, so there's no need to commit them separately. The reader can be reused across different transactions. However, it's essential to commit transactions in the same order as the messages are read from the reader, as message commits in the topic must be performed strictly in order - otherwise transaction will get an error during commit. The simplest way to ensure this is by using the reader within a loop.

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

  - Java (sync)

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadSync.java)

    A transaction can be set in `ReceiveSettings` for the `receive` method:


    ```java
    Message message = reader.receive(ReceiveSettings.newBuilder()
          .setTransaction(transaction)
          .build());
    ```


    A message received this way will be automatically committed with the provided transaction and shouldn't be committed directly.
    The `receive` method sends the `sendUpdateOffsetsInTransaction` request on the server to link the message offset with this transaction and blocks until a response is received.

  - Java (async)

    [Example on GitHub](https://github.com/ydb-platform/ydb-java-examples/blob/develop/ydb-cookbook/src/main/java/tech/ydb/examples/topic/transactions/TransactionReadAsync.java)

    In the `onMessages` callback, one or more messages can be linked with a transaction.
    To do that request `reader.updateOffsetsInTransaction` should be called. And transaction should not be committed until a response is received.
    This method needs a partition offsets list as a parameter.
    Such a list can be constructed manually or using the helper method `getPartitionOffsets()` that `Message` and `DataReceivedEvent` both provide.


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

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Processing a server read interrupt {#stop}

{{ ydb-short-name }} uses server-based partition balancing between clients. This means that the server can interrupt the reading of messages from random partitions.

In case of a _soft interruption_, the client receives a notification that the server has finished sending messages from the partition and messages will no longer be read. The client can finish processing messages and send a commit to the server.

In case of a _hard interruption_, the client receives a notification that it is no longer possible to work with partitions. The client must stop processing the read messages. Uncommited messages will be transferred to another consumer.

#### Soft reading interruption {#soft-stop}

{% list tabs group=lang %}

- C++

  A soft interrupt arrives as an `TStopPartitionSessionEvent` event with the `Confirm` method. The client can finish processing messages and send an acknowledgment to the server.

  Example of event loop fragment:


  ```cpp
  auto event = readSession->GetEvent(/*block=*/true);
  if (auto* stopPartitionSessionEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
      stopPartitionSessionEvent->Confirm();
  } else {
    // other event types
  }
  ```

- Go

  The client code immediately receives all messages from the buffer (on the SDK side) even if they are not enough to form a batch during batch processing.


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

  - Java (sync)

    Not relevant due to not being possible to change the way of handling such events.
    Client will automatically respond to server that it is ready to stop.

  - Java (async)

    To be able to react to such an event, override the `onStopPartitionSession(StopPartitionSessionEvent event)` method in the `ReadEventHandler` subclass object (see [Connecting to a topic for reading messages](#start-reader)).
    `event.confirm()` must be called, because the server expects this response to continue the shutdown.


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

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

#### Hard reading interruption {#hard-stop}

{% list tabs group=lang %}

- C++

  The hard interruption of reading messages is implemented using an `TPartitionSessionClosedEvent` event. It can be received either as soft interrupt confirmation response, or in the case of lost connection. The user can find out the reason for session closing using the `GetReason` method.

  Example of event loop fragment:


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

  When reading is interrupted, the message or message batch context is canceled.


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

  In this example, processing of messages within the batch will stop if the partition is reassigned during operation. This kind of optimization requires that you run extra code on the client side. In simple cases when processing of reassigned partitions is not a problem, you may skip this optimization.

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

  - Java (sync)

    Not relevant due to not being possible to change the way of handling such events.

  - Java (async)

    ```java
    @Override
    public void onPartitionSessionClosed(PartitionSessionClosedEvent event) {
      logger.info("Partition session {} is closed.", event.getPartitionSession().getPartitionId());
    }
    ```

  {% endlist %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Topic autoscaling {#autoscaling}

{% list tabs group=lang %}

- C++

  The SDK supports two topic reading modes with auto-scaling enabled: full support mode and compatibility mode. The reading mode is set in the read session creation parameters. By default, compatibility mode is used.


  ```cpp
  auto settings = NYdb::NTopic::TReadSessionSettings()
      .SetAutoscalingSupport(true); // full support is enabled

  // or

  auto settings = NYdb::NTopic::TReadSessionSettings()
      .SetAutoscalingSupport(false); // compatibility mode is enabled

  auto readSession = topicClient.CreateReadSession(settings);
  ```


  In full support mode, when all messages from a partition have been read, an `TEndPartitionSessionEvent` event is received. After receiving this event, no new messages will appear in the partition for reading. To continue reading from child partitions, call `Confirm()`, thereby confirming that the application is ready to accept messages from child partitions. If messages from all partitions are processed in a single thread, `Confirm()` can be called immediately after receiving `TEndPartitionSessionEvent`. If messages from different partitions are processed in different threads, you should finish processing the messages, for example, execute the accumulated batch, confirm their processing (commit), or save the read position in your database, and only then call `Confirm()`.

  Autoscaling of a topic can be enabled during its creation using the `TEndPartitionSessionEvent` option:

  When needed, you can set additional parameters in AutoPartitioningSettings:


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


  In compatibility mode, there is no explicit signal that reading from a partition is complete, and the server will try to heuristically determine that the client has processed the partition to the end. This may cause a delay between finishing reading from the original partition and starting to read from its child partitions.

  If the client confirms message processing (commit), the signal that processing messages from a partition is complete will be the confirmation of processing the last message of that partition. If the client does not confirm message processing, the server will periodically interrupt reading from the partition and switch to reading in another session (if there are other sessions ready to process the partition). This will continue until reading [starts](#client-commit) from the end of the partition.

  It is recommended to verify the correctness of handling the soft read interrupt: the client must process the received messages, confirm their processing (commit), or save the read position in its database, and only then call `Confirm()` for the `TStopPartitionSessionEvent` event.

- Go

  Enabling topic auto-scaling during its creation is done using the `topicoptions.CreateWithAutoPartitioningSettings` option:


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


  From a practical perspective, these modes do not differ for the end user. However, the full support mode differs from the compatibility mode in terms of who guarantees the order of reading—the client or the server. Compatibility mode is achieved through server-side processing and generally operates slower.


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


  Enabling auto-scaling for an existing topic is done using the `topicoptions.AlterWithAutoPartitioningStrategy` option of `.Topic().Alter`:


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


  The SDK supports two topic reading modes with auto-scaling enabled: full support mode and compatibility mode. The reading mode is set with the `topicoptions.WithReaderSupportSplitMergePartitions` option when creating the reader. By default, full support mode is used (`true`).


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

  Autoscaling of a topic can be enabled during its creation using the `auto_partitioning_settings` argument of `create_topic`:

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

  Changes to an existing topic can be made using the `alter_auto_partitioning_settings` argument of `alter_topic`:


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


  The SDK supports two topic reading modes with autoscaling enabled: full support mode and compatibility mode. The reading mode can be set in the `auto_partitioning_support` argument when creating the reader. Full support mode is used by default.


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


  From a practical perspective, these modes do not differ for the end user. However, the full support mode differs from the compatibility mode in terms of who guarantees the order of reading—the client or the server. Compatibility mode is achieved through server-side processing and generally operates slower.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Confirming processing outside the reader {#commit-outside-the-reader}

This functionality is not currently supported in the Go SDK.

{% list tabs group=lang %}

- Go

  Confirming processing outside the reader is done using the `db.Topic().CommitOffset` method:


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


  If there is an active read session at the time of confirmation (via `StartReader` or `StartListener`), it is recommended to pass its identifier using the `WithCommitOffsetReadSessionID` option. This allows the server not to interrupt the current read session:


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

  Commit outside the reader is done using the `topic_client.commit_offset` method:

  {% list tabs %}

  - Native SDK

    ```python
    driver.topic_client.commit_offset(
        topic_path,
        consumer_name,
        partition_id,
        offset,
        reader.read_session_id,  # optional: does not interrupt the active read session
    )
    ```

  - Native SDK (Asyncio)

    ```python
    await driver.topic_client.commit_offset(
        topic_path,
        consumer_name,
        partition_id,
        offset,
        reader.read_session_id,  # optional: does not interrupt the active read session
    )
    ```

  {% endlist %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  ```java
  TopicClient client = ...;

  String sessionID = reader.getSessionId();
  // For AsyncReader, the session identifier can be obtained when handling the SessionStartedEvent

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

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
