# Working with topics

This article provides examples of how to use the {{ ydb-short-name }} SDK to work with [topics](../../concepts/topic.md).

Before performing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Topic usage examples

{% list tabs %}

- C++

  [Reader example on GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/topic_reader)

- Go

  [Examples on GitHub](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples/topic)

- Java

  [Examples on GitHub](https://github.com/ydb-platform/ydb-java-examples/tree/master/ydb-cookbook/src/main/java/tech/ydb/examples/topic)

- Python

  [Examples on GitHub](https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/topic)

{% endlist %}

## Initializing a connection {#init}

{% list tabs %}

- C++

  To interact with YDB Topics, create an instance of the YDB driver and topic client.

  The YDB driver lets the app and YDB interact at the transport layer. The driver must exist during the YDB access lifecycle and be initialized before creating a client.

  Topic client ([source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1589)) requires the YDB driver for work. It handles topics and manages read and write sessions.

  App code snippet for driver initialization:
  ```cpp
  auto driverConfig = TDriverConfig()
      .SetEndpoint(opts.Endpoint)
      .SetDatabase(opts.Database)
      .SetAuthToken(GetEnv("YDB_TOKEN"));

  TDriver driver(driverConfig);
  ```

  This example uses authentication token from the `YDB_TOKEN` environment variable. For details see [Connecting to a database](../../concepts/connect.md) and [Authentication](../../concepts/auth.md) pages.

  App code snippet for creating a client:

  ```cpp
  TTopicClient topicClient(driver);
  ```

- Java

  To interact with YDB Topics, create an instance of the YDB transport and topic client.

  The YDB transport lets the app and YDB interact at the transport layer. The transport must exist during the YDB access lifecycle and be initialized before creating a client.

  App code snippet for transport initialization:
  ```java
  try (GrpcTransport transport = GrpcTransport.forConnectionString(connString)
          .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
          .build()) {
      // Use YDB transport
  }
  ```
  In this example `CloudAuthHelper.getAuthProviderFromEnviron()` helper method is used which retrieves auth token from environment variables.
  For example, `YDB_ACCESS_TOKEN_CREDENTIALS`.
  For details see [Connecting to a database](../../concepts/connect.md) and [Authentication](../../concepts/auth.md) pages.

  Topic client ([source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/TopicClient.java#L34)) uses YDB transport and handles all topics topic operations, manages read and write sessions.

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

{% endlist %}

## Managing topics {#manage}

### Creating a topic {#create-topic}

{% list tabs %}

The topic path is mandatory. Other parameters are optional.

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

   ```python
   driver.topic_client.create_topic(topic_path,
       supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
       min_active_partitions=3,                                    # optional
   )
   ```

- Java

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/settings/CreateTopicSettings.java#L97).

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

{% endlist %}

### Updating a topic {#alter-topic}

When you update a topic, you must specify the topic path and the parameters to be changed.

{% list tabs %}

- C++

  For a full list of supported parameters, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L458).

  Example of adding an [important consumer](../../concepts/topic#important-consumer) and setting two days [retention time](../../concepts/topic#retention-time) for the topic:

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

  ```python
  driver.topic_client.create_topic(topic_path,
      set_supported_codecs=[ydb.TopicCodec.RAW, ydb.TopicCodec.GZIP], # optional
      set_min_active_partitions=3,                                    # optional
  )
  ```

- Java

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

{% endlist %}

### Getting topic information {#describe-topic}

{% list tabs %}

- C++

  Use `DescribeTopic` method to get information about topic.

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
     log.Fatalf("failed drop topic: %v", err)
     return
   }
   fmt.Printf("describe: %#v\n", descResult)
   ```

- Python

   ```python
   info = driver.topic_client.describe_topic(topic_path)
   print(info)
   ```

- Java

  Use `describeTopic` method to get information about topic.

  For a full list of description fields, see the [source code](https://github.com/ydb-platform/ydb-java-sdk/blob/master/topic/src/main/java/tech/ydb/topic/description/TopicDescription.java#L19).

  ```java
  Result<TopicDescription> topicDescriptionResult = topicClient.describeTopic(topicPath)
          .join();
  TopicDescription description = topicDescriptionResult.getValue();
  ```

{% endlist %}

### Deleting a topic {#drop-topic}

To delete a topic, just specify the path to it.

{% list tabs %}

- C++

  ```cpp
  auto status = topicClient.DropTopic("my-topic").GetValueSync();
  ```

- Go

   ```go
     err := db.Topic().Drop(ctx, "topic-path")
   ```

- Python

   ```python
   driver.topic_client.drop_topic(topic_path)
   ```

- Java

  ```java
  topicClient.dropTopic(topicPath);
  ```

{% endlist %}

## Message writes {#write}

### Connecting to a topic for message writes {#start-writer}

Only connections with matching [producer and message group](../../concepts/topic#producer-id) identifiers are currently supported (`producer_id` shoud be equal to `message_group_id`). This restriction will be removed in the future.

{% list tabs %}

- C++

  The write session object with `IWriteSession` interface is used to connect to a topic for writing.

  For a full list of write session settings, see the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1199).

  Example of creating a write session:

  ```cpp
  TString producerAndGroupID = "group-id";
  auto settings = TWriteSessionSettings()
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

   ```python
   writer = driver.topic_client.writer(topic_path)
   ```

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
          logger.info("Init finished succsessfully");
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

### Writing messages {#writing-messages}

{% list tabs %}

- C++

  `IWriteSession` interface allows asynchronous write.

  The user processes three kinds of events in a loop: `TReadyToAcceptEvent`, `TAcksEvent`, and `TSessionClosedEvent`.

  For each kind of event user can set a handler in write session settings before session creation. Also, a common handler can be set.

  If handler is not set for a particular event, it will be delivered to SDK client via `GetEvent` / `GetEvents` methods. `WaitEvent` method allows user to await for a next event in non-blocking way with `TFuture<void>()` interface.

  To write a message, user uses a move-only `TContinuationToken` object, which has been created by the SDK and has been delivered to the user with a `TReadyToAcceptEvent` event. During write user can set an arbitrary sequential number and a message creation timestamp. By default they are generated by the SDK.

  `Write` is asynchronous. Data from messages is processed and stored in the internal buffer. Settings `MaxMemoryUsage`, `MaxInflightCount`, `BatchFlushInterval`, and `BatchFlushSizeBytes` control sending in the background. Write session reconnects to the YDB if the connection fails and resends the message if possible, with regard to `RetryPolicy` setting. If an error that cannot be repeated is received, write session stops and sends `TSessionClosedEvent` to the client.

  Example of writing using event loop without any handlers set up:
  ```cpp
  // Event loop
  while (true) {
      // Get event
      // May block for a while if write session is busy
      TMaybe<TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);

      if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
          session->Write(std::move(event.ContinuationToken), "This is yet another message.");

      } else if (auto* ackEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
          std::cout << ackEvent->DebugString() << std::endl;

      } else if (auto* closeSessionEvent = std::get_if<TSessionClosedEvent>(&*event)) {
          break;
      }
  }
  ```

- Go

   To send a message, just save Reader in the Data field, from which the data can be read. You can expect the data of each message to be read once (or until the first error). By the time you return the data from Write, it will already have been read and stored in the internal buffer.

   By default, SeqNo and the message creation date are set automatically.

   By default, Write is performed asynchronously: data from messages is processed and stored in the internal buffer, sending is done in the background. Writer reconnects to the YDB if the connection fails and resends the message if possible. If an error that cannot be repeated is received , Writer stops and subsequent Write calls will end with an error.

   ```go
   err := writer.Write(ctx,
     topicwriter.Message{Data: strings.NewReader("1")},
     topicwriter.Message{Data: bytes.NewReader([]byte{1,2,3})},
     topicwriter.Message{Data: strings.NewReader("3")},
   )
   if err == nil {
     return err
   }
   ```

- Python

   To deliver messages, you can either simply transmit message content (bytes, str) or set certain properties manually. You can send objects one-by-one or as a list. The `write` method is asynchronous. The method returns immediately once messages are put to the client's internal buffer; this is usually a fast process. If the internal buffer is filled up, you might need to wait until part of the data is sent to the server.

   ```python
   # Simple delivery of messages, without explicit metadata.
   # Easy to get started, easy to use if everything you need is the message content.
   writer = driver.topic_client.writer(topic_path)
   writer.write("mess")  # Rows will be transmitted in UTF-8; this is the easiest way to send
                         # text messages.
   writer.write(bytes([1, 2, 3]))  # These bytes will be transmitted as they are, this is the easiest way to send
                                   # binary data.
   writer.write(["mess-1", "mess-2"])  # This line multiple messages per call
                                       # to decrease overheads on internal SDK processes.
                                       # This makes sense when the message stream is high.

   # This is the full form; it is used when except the message content you need to manually specify its properties.
   writer = driver.topic_client.writer(topic="topic-path", auto_seqno=False, auto_created_at=False)

   writer.write(ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()))
   writer.write(ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now()))

   # In the full form, you can also send multiple messages per function call.
   # This approach is useful when the message stream is high, and you want to
   # reduce overheads on SDK internal calls.
   writer.write([
     ydb.TopicWriterMessage("asd", seqno=123, created_at=datetime.datetime.now()),
     ydb.TopicWriterMessage(bytes([1, 2, 3]), seqno=124, created_at=datetime.datetime.now(),
     ])

   ```

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

  Method `send` puts a message into writer's sending queue.
  Method returns `CompletableFuture<WriteAck>` which allows checking if the message was really written.
  In case if the queue is full, `QueueOverflowException` exception will be thrown.
  It is a way to signal a user that writing speed should be slowed down.
  In this case a message write should be skipped or retried with exponential backoff.
  Client buffer size can be also increased (`setMaxSendBufferMemorySize`) to be able to store more messages in memory before this exception is thrown.

  ```java
  try {
      // Non-blocking. Throws QueueOverflowException if send queue is full
      writer.send(Message.of("33".getBytes()));
  } catch (QueueOverflowException exception) {
      // Send queue is full. Need to retry with backoff or skip
  }
  ```

{% endlist %}

### Message writes with storage confirmation on the server

{% list tabs %}

- C++

  `IWriteSession` interface allows getting server acknowledgments for writes.

  Status of server-side message write is represented with `TAcksEvent`. One event can contain the statuses of several previously sent messages.Status is one of the following: message write is confirmed (`EES_WRITTEN`), message is discarded as a duplicate of a previously written message (`EES_ALREADY_WRITTEN`) or message is discarded because of failure (`EES_DISCARDED`).

  Example of setting TAcksEvent handler for a write session:
  ```cpp
  auto settings = TWriteSessionSettings()
    // other settings are set here
    .EventHandlers(
      TWriteSessionSettings::TEventHandlers()
        .AcksHandler(
          [&](TWriteSessionEvent::TAcksEvent& event) {
            for (const auto& ack : event.Acks) {
              if (ack.State == TWriteAck::EEventState::EES_WRITTEN) {
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
   if err == nil {
     return err
   }
   ```

- Python

   There are two ways to get a message write acknowledgement from the server:

   * `flush()`: Waits until all the messages previously written to the internal buffer are acknowledged.
   * `write_with_ack(...)`: Sends a message and waits for the acknowledgement of its delivery from the server. This method is slow when you are sending multiple messages in a row.

   ```python
   # Put multiple messages to the internal buffer and then wait
   # until all of them are delivered to the server.
   for mess in messages:
       writer.write(mess)

   writer.flush()

   # You can send multiple messages and wait for an acknowledgment for the entire group.
   writer.write_with_ack(["mess-1", "mess-2"])

   # Waiting on sending each message: this method will return the result only after an
   # acknowledgment from the server.
   # This is the slowest message delivery option; use it when this mode is
   # absolutely needed.
   writer.write_with_ack("message")

   ```

- Java (sync)

  Blocking method `flush()` waits until all the messages previously written to the internal buffer are acknowledged:

  ```java
  for (byte[] message : messages) {
      writer.send(Message.of(message));
  }
  writer.flush();
  ```

- Java (async)

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

{% endlist %}

### Selecting a codec for message compression {#codec}

For more details on using data compression for topics, see [here](../../concepts/topic#message-codec).


{% list tabs %}

- C++

  The message compression can be set on the [write session creation](#start-writer) with `Codec` and `CompressionLevel` settings. By default, GZIP codec is chosen.

  Example of creating a write session with no data compression:

  ```cpp
  auto settings = TWriteSessionSettings()
    // other settings are set here
    .Codec(ECodec::RAW);

  auto session = topicClient.CreateWriteSession(settings);
  ```

  Write session allows sending a message compressed with other codec. For this use `WriteEncoded` method, specify codec used and original message byte size. The codec must be allowed in topic settings.

- Go

   By default, the SDK selects the codec automatically based on topic settings. In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then it sometimes tries to compress messages with all the available codecs, and then selects the codec that yields the smallest message size. If the list of allowed codecs for the topic is empty, the SDK makes automatic selection between Raw and Gzip codecs.

   If necessary, a fixed codec can be set in the connection options. It will then be used and no measurements will be taken.

   ```go
   producerAndGroupID := "group-id"
   writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
     topicoptions.WithMessageGroupID(producerAndGroupID),
     topicoptions.WithCodec(topictypes.CodecGzip),
   )
   ```

- Python

   By default, the SDK selects the codec automatically based on topic settings. In automatic mode, the SDK first sends one group of messages with each of the allowed codecs, then it sometimes tries to compress messages with all the available codecs, and then selects the codec that yields the smallest message size. If the list of allowed codecs for the topic is empty, the SDK makes automatic selection between Raw and Gzip codecs.

   If necessary, a fixed codec can be set in the connection options. It will then be used and no measurements will be taken.

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

{% endlist %}

### Writing messages in no-deduplication mode

{% list tabs %}

- C++

If no ProducerId is specified on write session setup, the session runs in no-deduplication mode. The example below deminstrates such a session setup:

```cpp
  auto settings = TWriteSessionSettings()
      .Path(myTopicPath);

  auto session = topicClient.CreateWriteSession(settings);
  ```

If, on other hand, you want to ensure deduplication is enabled, you can specify the ProducerId option or call the `EnableDeduplication()` method from WriteSessionSettings. The '[Connecting to a topic for message writes](#start-writer)' section has an example of write session that has deduplication enabled.

{% endlist %}

### Using message metadata feature {#messagemeta}

You can provide some metadata for any particular message when writing. This metadata can be a list of up to 100 key-value pairs per message.
All the metadata provided when writing a message is sent to a consumer with the message during reading.

{% list tabs %}

- C++

  To take advantage of message metadata feature, use the `Write()` method with `TWriteMessage`argument as below:

  ```cpp
  auto settings = TWriteSessionSettings()
      .Path(myTopicPath)
  //set all oter settings;
  ;

  auto session = topicClient.CreateWriteSession(settings);

  TMaybe<TWriteSessionEvent::TEvent> event = session->GetEvent(/*block=*/true);
  TWriteMessage message("This is yet another message").MessageMeta({
      {"meta-key", "meta-value"},
      {"another-key", "value"}
  });

  if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
      session->Write(std::move(event.ContinuationToken), std::move(message));
  };

  ```

- Java

  Construct messages with the builder to take advantage of the message metadata feature. You can add `MetadataItem` objects to a message. Each item consists of a key of type `String` and a value of type `byte[]`.

  `MetadataItem`s can be set as a `List`:

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

  Or each `MetadataItem` can be added individually:

  ```java
  writer.send(
          Message.newBuilder()
                  .addMetadataItem(new MetadataItem("meta-key", "meta-value".getBytes()))
                  .addMetadataItem(new MetadataItem("another-key", "value".getBytes()))
                  .build()
  );
  ```

  While reading, metadata can be received from a `Message` with the `getMetadataItems()` method:

  ```java
  Message message = reader.receive();
  List<MetadataItem> metadata = message.getMetadataItems();
  ```

{% endlist %}

### Write in a transaction {#write-tx}

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

  // flush to wait until all messages reach server
  writer.flush();

  Status commitStatus = transaction.commit().join();
  analyzeCommitStatus(commitStatus);
  ```

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

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

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

{% endlist %}

## Reading messages {#reading}

### Connecting to a topic for message reads {#start-reader}

To be able to read messages from topic, a Consumer on this topic should exist.
A Consumer can be created on [creating](#create-topic) or [altering](#alter-topic) a topic.
Topic can have several Consumers and for each of them server stores its own reading progress.

{% list tabs %}

- C++

  The read session object with `IReadSession` interface is used to connect to one or more topics for reading.

  For a full list of read session settings, see `TReadSessionSettings` class in the [source code](https://github.com/ydb-platform/ydb/blob/d2d07d368cd8ffd9458cc2e33798ee4ac86c733c/ydb/public/sdk/cpp/client/ydb_topic/topic.h#L1344).

  To establish a connection to the existing `my-topic` topic using the added `my-consumer` consumer, use the following code:

  ```cpp
  auto settings = TReadSessionSettings()
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

   ```python
   reader = driver.topic_client.reader(topic="topic-path", consumer="consumer_name")
   ```

- Java (sync)

  Reader settings initialization:
  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
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
          logger.info("Init finished succsessfully");
      } catch (Exception exception) {
          logger.error("Exception while initializing reader: ", exception);
          return;
      }
      ```

- Java (async)

  Reader settings initialization:
  ```java
  ReaderSettings settings = ReaderSettings.newBuilder()
          .setConsumerName(consumerName)
          .addTopic(TopicReadSettings.newBuilder()
                  .setPath(topicPath)
                  .setReadFrom(Instant.now().minus(Duration.ofHours(24))) // Optional
                  .setMaxLag(Duration.ofMinutes(30)) // Optional
                  .build())
          .build();
  ```

  For async reader, `ReadEventHandlersSettings` also have to be provided with an implementation of `ReadEventHandler`.
  It describes how events should be handled during reading.
  ```java
  ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
          .setEventHandler(new Handler())
          .build();
  ```
  Optionally, an executor for message handling can be also provided in `ReadEventHandlersSettings`.
  To implement a Handler, default abstract class `AbstractReadEventHandler` can be used.
  It is enough to override the `onMessages` method that describes message handling. Implementation example:
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

Additional options are used to specify multiple topics and other parameters.
To establish a connection to the `my-topic` and `my-specific-topic` topics using the `my-consumer` consumer and also set the time to start reading messages, use the following code:

{% list tabs %}

- C++

  ```cpp
  auto settings = TReadSessionSettings()
      .ConsumerName("my-consumer")
      .AppendTopics("my-topic")
      .AppendTopics(
          TTopicReadSettings("my-specific-topic")
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

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [consumer offset](../../concepts/topic.md#consumer-offset). After reading a message, the client should [send a commit to the server](#commit). The consumer offset changes and only uncommitted messages will be read in case of a new connection.

You can read messages without a [commit](#no-commit) as well. In this case, all uncommited messages, including those processed, will be read if there is a new connection.

Information about which messages have already been processed can be [saved on the client side](#client-commit) by sending the starting consumer offset to the server when creating a new connection. This does not change the consumer offset on the server.

Data from topics can be read in the context of [transactions](#read-tx). In this case, the reading offset will only advance when the transaction is committed. On reconnect, all uncommitted messages will be read again.

{% list tabs %}

- C++

  The user processes several kinds of events in a loop: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent` and `TSessionClosedEvent`.

  For each kind of event user can set a handler in read session settings before session creation. Also, a common handler can be set.

  If handler is not set for a particular event, it will be delivered to SDK client via `GetEvent` / `GetEvents` methods. The `WaitEvent` method allows user to await for a next event in non-blocking way with `TFuture<void>()` interface.

- Go

  The SDK receives data from the server in batches and buffers it. Depending on the task, the client code can read messages from the buffer one by one or in batches.

- Python

  The SDK receives data from the server in batches and buffers it. Depending on the task, the client code can read messages from the buffer one by one or in batches.

- Java

  The SDK receives data from the server in batches and buffers it. Depending on the task, the client code can read messages from the buffer one by one or in batches.

{% endlist %}


### Reading without a commit {#no-commit}

#### Reading messages one by one

{% list tabs %}

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

   ```python
   while True:
       message = reader.receive_message()
       process(message)
   ```

- Java (sync)

  To read messages one-by-one without commit just do not call the `commit` method on messages:

  ```java
  while(true) {
      Message message = reader.receive();
      process(message);
  }
  ```

- Java (async)

  Reading messages one-by-one is not supported in async Reader.

{% endlist %}

#### Reading message batches

{% list tabs %}

- C++

  One simple way to read messages is to use `SimpleDataHandlers` setting when creating a read session. With it you only set a handler for a `TDataReceivedEvent`. SDK will call it for each batch of messages that came from server. By default, SDK does not send back acknowledgments of successful reads.

  ```cpp
  auto settings = TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << DebugString(event);
          }
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  ReadSession->GetEvent(/* block = */true);
  ```

  In this example client creates read session and just awaits session close in the main thread. All other event types are handled by SDK.

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

   ```python
   while True:
     batch = reader.receive_batch()
     process(batch)
   ```

- Java (sync)

  Reading messages in batches is not supported in sync Reader.

- Java (async)

  To read messages without commit just do not call the `commit` method:

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

### Reading with a commit {#commit}

Confirmation of message processing (commit) informs the server that the message from the topic has been processed by the recipient and does not need to be sent anymore. When using acknowledged reading, it is necessary to confirm all received messages without skipping any. Message commits on the server occur after confirming a consecutive interval of messages "without gaps," and the confirmations themselves can be sent in any order.

For example, if messages 1, 2, 3 are received from the server, the program processes them in parallel and sends confirmations in the following order: 1, 3, 2. In this case, message 1 will be committed first, and messages 2 and 3 will be committed only after the server receives confirmation of the processing of message 2.

If a commit fails with an error, the application should log it and continue; it makes no sense to retry the commit. At this point, it is not known if the message was actually confirmed.

#### Reading messages one by one with commits

{% list tabs %}

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

- Python

   ```python
   while True:
       message = reader.receive_message()
       process(message)
       reader.commit(message)
   ```

- Java

  To commit a message just call `commit` method on it.
  This method returns `CompletableFuture<Void>` which successful completion means that the server confirmed commit.
  In case of an error on commit do not retry it. Most likely, an error is caused be session shutdown.
  The reader (maybe another one) will create a new session for this partition and the message will be read again.

  ```java
  message.commit()
         .whenComplete((result, ex) -> {
             if (ex != null) {
                 // Read session was probably closed, there is nothing we can do here.
                 // Do not retry this commit on the same message.
                 logger.error("exception while committing message: ", ex);
             } else {
                 logger.info("message committed successfully");
             }
         });
  ```

{% endlist %}

#### Reading message batches with commits

{% list tabs %}

- C++

  Same as [above example](#no-commit), when using `SimpleDataHandlers` handlers you only set handler for a `TDataReceivedEvent`. SDK will call it for each batch of messages that came from server. By setting `commitDataAfterProcessing = true`, you tell SDK to send back commits after executing a handler for corresponding event.

  ```cpp
  auto settings = TReadSessionSettings()
      .EventHandlers_.SimpleDataHandlers(
          [](TReadSessionEvent::TDataReceivedEvent& event) {
              std::cout << "Get data event " << DebugString(event);
          }
          , /* commitDataAfterProcessing = */true
      );

  auto session = topicClient.CreateReadSession(settings);

  // Wait SessionClosed event.
  ReadSession->GetEvent(/* block = */true);
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

- Python

   ```python
   while True:
     batch = reader.receive_batch()
     process(batch)
     reader.commit(batch)
   ```

- Java (sync)

  Not relevant due to sync reader only reading messages one by one.

- Java (async)

  In `onMessage` handler whole message batch in `DataReceivedEvent` can be committed:

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
                     // Do not retry this commit on the same event.
                     logger.error("exception while committing message batch: ", ex);
                 } else {
                     logger.info("message batch committed successfully");
                 }
             });
  }
  ```

{% endlist %}

### Reading with consumer offset storage on the client side {#client-commit}

Instead of committing messages, the client application may track reading progress on its own. In this case, it can provide a handler, which will be called back on each partition read start. This handler may set a starting reading position for this partition.

{% list tabs %}

- C++

  The starting position of a specific partition read can be set during `TStartPartitionSessionEvent` handling.
  For this purpose, `TStartPartitionSessionEvent::Confirm` has a `readOffset` parameter.
  Additionally, there is a `commitOffset` parameter that tells the server to consider all messages with lesser offsets [committed](#commit).

  Setting handler example:
  ```cpp
  settings.EventHandlers_.StartPartitionSessionHandler(
      [](TReadSessionEvent::TStartPartitionSessionEvent& event) {
          auto readFromOffset = GetOffsetToReadFrom(event.GetPartitionId());
          event.Confirm(readFromOffset);
      }
  );
  ```
  In the code above,`GetOffsetToReadFrom` is part of the example, not SDK. Use your own method to provide the correct starting offset for a partition with a given partition id.

  Also, `TReadSessionSettings` has a `ReadFromTimestamp` setting for reading only messages newer than the given timestamp. This setting is intended to skip some messages, not for precise reading start positioning. Several first-received messages may still have timestamps less than the specified one.

- Go

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

  The starting offset for reading in Java can only be set for AsyncReader.
  In `StartPartitionSessionEvent`, a `StartPartitionSessionSettings` object with the desired ReadOffset can be passed to the `confirm` method.
  The offset that should be considered as committed can be set with the `setCommittedOffset` method.

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

{% endlist %}

### Reading without a Consumer {#no-consumer}
Reading progress is usually saved on a server for each Consumer. However, such progress can't be saved if a reader is created without a specified `Consumer`.

{% list tabs %}

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

{% endlist %}

### Reading in a transaction {#read-tx}

{% list tabs %}

- C++

  Before reading messages, the client code must pass a transaction object reference to the reading session settings.

  ```cpp
      ReadSession->WaitEvent().Wait(TDuration::Seconds(1));

      auto tableSettings = NYdb::NTable::TTxSettings::SerializableRW();
      auto transactionResult = TableSession->BeginTransaction(tableSettings).GetValueSync();
      auto Transaction = transactionResult.GetTransaction();

      NYdb::NTopic::TReadSessionGetEventSettings topicSettings;
      topicSettings.Block(false);
      topicSettings.Tx(Transaction);

      auto events = ReadSession->GetEvents(topicSettings);

      for (auto& event : events) {
          // process the event and write results to a table
      }

      NYdb::NTable::TCommitTxSettings commitSettings;
      auto commitResult = Transaction.Commit(commitSettings).GetValueSync();
  ```

{% note warning %}

  When processing `events`, you do not need to confirm processing for `TDataReceivedEvent` events explicitly.

{% endnote %}

  Confirmation of the `TStopPartitionSessionEvent` event processing must be done after calling `Commit`.

  ```cpp
      std::optional<TStopPartitionSessionEvent> stopPartitionSession;

      auto events = ReadSession->GetEvents(topicSettings);

      for (auto& event : events) {
          if (auto* e = std::get_if<TStopPartitionSessionEvent>(&event) {
              stopPartitionSessionEvent = std::move(*e);
          } else {
             // process the event and write results to a table
          }
      }

      NYdb::NTable::TCommitTxSettings commitSettings;
      auto commitResult = Transaction.Commit(commitSettings).GetValueSync();

      if (stopPartitionSessionEvent) {
          stopPartitionSessionEvent->Commit();
      }
  ```

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

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

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

  {% include [java_transaction_requirements](_includes/alerts/java_transaction_requirements.md) %}

{% endlist %}


### Processing a server read interrupt {#stop}

{{ ydb-short-name }} uses server-based partition balancing between clients. This means that the server can interrupt the reading of messages from random partitions.

In case of a _soft interruption_, the client receives a notification that the server has finished sending messages from the partition and messages will no longer be read. The client can finish processing messages and send a commit to the server.

In case of a _hard interruption_, the client receives a notification that it is no longer possible to work with partitions. The client must stop processing the read messages. Uncommited messages will be transferred to another consumer.

#### Soft reading interruption {#soft-stop}

{% list tabs %}

- C++

  The `TStopPartitionSessionEvent` class is used for soft reading interruption. It helps user to stop message processing gracefully.

  Example of event loop fragment:

  ```cpp
  auto event = ReadSession->GetEvent(/*block=*/true);
  if (auto* stopPartitionSessionEvent = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
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

   ```python
   while True:
     batch = reader.receive_batch()
     process(batch)
     reader.commit(batch)
   ```

- Java (sync)

  Not relevant due to not being possible to change the way of handling such events.
  Client will automatically respond to server that it is ready to stop.

- Java (async)

  `onStopPartitionSession(StopPartitionSessionEvent event)` handler should be overridden to handle this event:

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

#### Hard reading interruption {#hard-stop}

{% list tabs %}

- C++

  The hard interruption of reading messages is implemented using an `TPartitionSessionClosedEvent` event. It can be received either as soft interrupt confirmation response, or in the case of lost connection. The user can find out the reason for session closing using the `GetReason` method.

  Example of event loop fragment:

  ```cpp
  auto event = ReadSession->GetEvent(/*block=*/true);
  if (auto* partitionSessionClosedEvent = std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
      if (partitionSessionClosedEvent->GetReason() == TPartitionSessionClosedEvent::EReason::ConnectionLost) {
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
