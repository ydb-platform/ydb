<!-- This file is not tracked by the automatic translation system. Edits to the RU-version must be made yourself. -->
# Working with topics

This article provides examples of how to use the {{ ydb-short-name }} SDK to work with [topics](../../concepts/topic.md).

Before performing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

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

  This example uses authentication token from the `YDB_TOKEN` environment variable. For details see [Connecting to a database](../../concepts/connect.md) и [Authentication](../../concepts/auth.md) pages.

  App code snippet for creating a client:

  ```cpp
  TTopicClient topicClient(driver);
  ```

{% endlist %}

## Managing topics {#manage}

### Creating a topic {#create-topic}

{% list tabs %}

- C++

  The topic path is mandatory. Other parameters are optional.

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

  The topic path is mandatory. Other parameters are optional.

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

   This feature is under development.

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

{% endlist %}

### Asynchronous message writes {#async-write}

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

{% endlist %}

## Reading messages {#reading}

### Connecting to a topic for message reads {#start-reader}

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

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [consumer offset](../../concepts/topic.md#consumer-offset). After reading a message, the client should [send a commit to the server](#commit). The consumer offset changes and only uncommitted messages will be read in case of a new connection.

You can read messages without a [commit](#no-commit) as well. In this case, all uncommited messages, including those processed, will be read if there is a new connection.

Information about which messages have already been processed can be [saved on the client side](#client-commit) by sending the starting consumer offset to the server when creating a new connection. This does not change the consumer offset on the server.

{% list tabs %}

- C++

  The user processes several kinds of events in a loop: `TDataReceivedEvent`, `TCommitOffsetAcknowledgementEvent`, `TStartPartitionSessionEvent`, `TStopPartitionSessionEvent`, `TPartitionSessionStatusEvent`, `TPartitionSessionClosedEvent` and `TSessionClosedEvent`.

  For each kind of event user can set a handler in read session settings before session creation. Also, a common handler can be set.

  If handler is not set for a particular event, it will be delivered to SDK client via `GetEvent` / `GetEvents` methods. `WaitEvent` method allows user to await for a next event in non-blocking way with `TFuture<void>()` interface.

- Go

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

{% endlist %}

### Reading with a commit {#commit}

Confirmation of message processing (commit) informs the server that the message from the topic has been processed by the recipient and does not need to be sent anymore. When using acknowledged reading, it is necessary to confirm all received messages without skipping any. Message commits on the server occur after confirming a consecutive interval of messages "without gaps," and the confirmations themselves can be sent in any order.

For example, if messages 1, 2, 3 are received from the server, the program processes them in parallel and sends confirmations in the following order: 1, 3, 2. In this case, message 1 will be committed first, and messages 2 and 3 will be committed only after the server receives confirmation of the processing of message 2.

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

{% endlist %}

### Reading with consumer offset storage on the client side {#client-commit}

When reading starts, the client code must transmit the starting consumer offset to the server:

{% list tabs %}

- C++

  Setting the starting offset for reading is not supported in the current C++ SDK.

  The `ReadFromTimestamp` setting is used for reading only messages with write timestamps no less than the given one.

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

{% endlist %}
