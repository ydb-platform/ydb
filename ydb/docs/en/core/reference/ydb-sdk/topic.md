# Working with topics

This article provides examples of how to use the {{ ydb-short-name }} SDK to work with [topics](../../concepts/topic.md).

Before performing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Managing topics
### Creating a topic

{% list tabs %}

The only mandatory parameter for creating a topic is its path, other parameters are optional.

- Go

   For a full list of supported parameters, see the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#CreateOption).

   Example of creating a topic with a list of supported codecs and a minimum number of partitions
   ```go
   err := db.Topic().Create(ctx, "topic-path",
       // optional
   	  topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

   	  // optional
   	  topicoptions.CreateWithMinActivePartitions(3),
   )
   ```

{% endlist %}

### Updating a topic

When you update a topic, you must specify the topic path and the parameters to be changed.

{% list tabs %}
- Go

   For a full list of supported parameters, see the [SDK documentation](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions#AlterOption).

   Example of adding a consumer to a topic

   ```go
    err := db.Topic().Alter(ctx, "topic-path",
   		topicoptions.AlterWithAddConsumers(topictypes.Consumer{
   			Name:            "new-consumer",
   			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
   		}),
   	)
   ```
{% endlist %}

### Getting topic information

{% list tabs %}

- Go

   ```go
     descResult, err := db.Topic().Describe(ctx, "topic-path")
   	if err != nil {
   		log.Fatalf("failed drop topic: %v", err)
   		return
   	}
   	fmt.Printf("describe: %#v\n", descResult)
   ```

{% endlist %}

### Deleting a topic

To delete a topic, just specify the path to it.

{% list tabs %}

- Go

   ```go
     err := db.Topic().Drop(ctx, "topic-path")
   ```

{% endlist %}


## Message writes
### Connecting to a topic for message writes {#start-writer}

Only connections with matching producer_id and message_group_id are currently supported. This restriction will be removed in the future.

{% list tabs %}

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

{% endlist %}


### Asynchronous message writes

{% list tabs %}

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

{% endlist %}


### Message writes with storage confirmation on the server

{% list tabs %}

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

{% endlist %}

### Selecting a codec for message compression

{% list tabs %}

- Go

   By default, the SDK selects the codec automatically (subject to topic settings). In automatic mode, the SDK will first send one group of messages with each of the allowed codecs, then sometimes try to compress messages with all available codecs, and select the codec that provides the smallest message size. If the list of allowed codecs for the topic is empty, auto-select is made between Raw and Gzip codecs.
   If necessary, a fixed codec can be set in the connection options. It will then be used and no measurements will be taken.

   ```go
   producerAndGroupID := "group-id"
   writer, _ := db.Topic().StartWriter(producerAndGroupID, "topicName",
     topicoptions.WithMessageGroupID(producerAndGroupID),
     topicoptions.WithCodec(topictypes.CodecGzip),
   )
   ```

{% endlist %}



## Message reads
### Connecting to a topic for message reads {#start-reader}

To create a connection to the existing `my-topic` topic via the added `my-consumer` consumer, use the following code:

{% list tabs %}

- Go

   ```go
   reader, err := db.Topic().StartReader("my-consumer", topicoptions.ReadTopic("my-topic"))
   if err != nil {
       return err
   }
   ```

{% endlist %}

You can also use the advanced connection creation option to specify multiple topics and set read parameters. The following code will create a connection to the `my-topic` and `my-specific-topic` topics via the `my-consumer` consumer and also set the time to start reading messages:

{% list tabs %}

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

{% endlist %}

### Reading messages {#reading-messages}

The server stores the [consumer offset](../../concepts/topic.md#consumer-offset). After reading a message, the client can [send a commit to the server](#commit). The consumer offset will change and only uncommitted messages will be read in case of a new connection.

You can read messages without a [commit](#no-commit) as well. In this case, all uncommited messages, including those processed, will be read if there is a new connection.

Information about which messages have already been processed can be [saved on the client side](#client-commit) by sending the starting consumer offset to the server when creating a new connection. This does not change the consumer offset on the server.

The SDK receives data from the server in batches and buffers it. Depending on the task, the client code can read messages from the buffer one by one or in batches.

### Reading without a commit {#no-commit}

To read messages one by one, use the following code:

{% list tabs %}

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

{% endlist %}

To read message batches, use the following code:

{% list tabs %}

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

{% endlist %}

#### Reading with a commit {#commit}

To commit messages one by one, use the following code:

{% list tabs %}

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

{% endlist %}

To commit message batches, use the following code:

{% list tabs %}

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

{% endlist %}

#### Reading with consumer offset storage on the client side {#client-commit}

When reading starts, the client code must transmit the starting consumer offset to the server:

{% list tabs %}

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

{% endlist %}

### Processing a server read interrupt {#stop}

{{ ydb-short-name }} uses server-based partition balancing between clients. This means that the server can interrupt the reading of messages from random partitions.

In case of a _soft interruption_, the client receives a notification that the server has finished sending messages from the partition and messages will no longer be read. The client can finish processing messages and send a commit to the server.

In case of a _hard interruption_, the client receives a notification that it is no longer possible to work with partitions. The client must stop processing the read messages. Uncommited messages will be transferred to another consumer.

#### Soft reading interruption {#soft-stop}

{% list tabs %}

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

{% endlist %}

#### Hard reading interruption {#hard-stop}

{% list tabs %}

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

{% endlist %}
