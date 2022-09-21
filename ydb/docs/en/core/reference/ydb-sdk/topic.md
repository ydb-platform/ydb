# Working with topics

This article provides examples of how to use the {{ ydb-short-name }} SDK to work with [topics](../../concepts/topic.md).

Before performing the examples, [create a topic](../ydb-cli/topic-create.md) and [add a consumer](../ydb-cli/topic-consumer-add.md).

## Connecting to a topic {#start-reader}

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

## Reading messages {#reading-messages}

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

### Reading with a commit {#commit}

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

### Reading with consumer offset storage on the client side {#client-commit}

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

## Processing a server read interrupt {#stop}

{{ ydb-short-name }} uses server-based partition balancing between clients. This means that the server can interrupt the reading of messages from random partitions.

In case of a _soft interruption_, the client receives a notification that the server has finished sending messages from the partition and messages will no longer be read. The client can finish processing messages and send a commit to the server.

In case of a _hard interruption_, the client receives a notification that it is no longer possible to work with partitions. The client must stop processing the read messages. Uncommited messages will be transferred to another consumer.

### Soft reading interruption {#soft-stop}

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

### Hard reading interruption {#hard-stop}

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
