# Working with topics
## Concepts
### Message
The minimum atomic unit of user information. Consists of a message body, properties, and write session attributes.

### Message body
An arbitrary set of bytes that YDB doesn't interpret in any way.

### Message properties
Typed message fields received beyond the main message body and having a predefined meaning.

#### codec
Message encoding method. Usually the used compression algorithm is specified. SDK will use the respective message decompression algorithm before passing it to the client code.

#### created_at
Message creation time, set by the producer and passed to consumers as is, without verifying it on the server side.

#### message_group_id
Set by the producer optionally, used for message partitioning.

#### offset
Message sequence number within a partition, assigned by the server when saving the message. The offset of the first message in a partition is equal to 0, then it increases. Some offset values may be skipped.

#### uncompressed_size
Decompressed message size, set by the producer and passed to consumers as is, without verifying it on the server side.

#### seq_no
Message sequence number within a single ProducerID. Set by the message author before sending it to the server.
Sequence numbers must be set within ProducerIDs in ascending order.

#### producer_id
ID set by the producer. For each producer_id within a partition, the seq_no ascending order is guaranteed.

#### written_at
Time when a message is written on the server. Set by the server while saving the message.

#### write_session_meta
An array of string key/value attributes set by the producer when starting a write session. Session attributes will be the same for all messages written within a single session.

### Message commits
Committing the fact of message processing by a consumer. Indicates that the consumer has handled the message and no longer needs it. Message commits are independent for different consumers.

### Topic
A named set of messages. Messages are written and read using topics.

### Partition
A topic scaling unit. Partitions within topics are numbered starting with 0. Messages are eventually saved to partitions. Within a partition, messages are ordered and numbered.

### Consumer
A named entity that reads data from a topic. A consumer contains committed read positions that are saved on the server side.

### Important consumer
A consumer flagged as "important". This flag indicates that a message won't be removed from a topic until an important consumer commits that it's handled, even if it's time to remove it based on the rotation rules. If it takes an important consumer too long to read a message, you can totally run out of disk space.

## Guarantees
In general, at-least-once message delivery is guaranteed.

### Message writes
1. After a write is committed on the server side, it's considered that a message is saved safely and will be delivered to consumers.
2. Messages with the same message_group_id get into the same partition.
3. If the message_group_id and partition are not set explicitly, messages with the same producer_id get into the same partition.
4. When writing messages to partitions, their order is maintained within a single producer_id.
5. If, when writing a message to a partition, its seq_no is less than or equal to the seq_no of a previously committed message for the same producer_id, this message is skipped and not written.

### Message reads
1. From each partition, messages arrive in the offset ascending order.
2. From each partition, messages arrive with the seq_no ascending within the same producer_id.
3. Once the server acknowledges a message commit, it will no longer be sent to this consumer.

## Working with topics from SDK
### Connecting to a topic
To read a message from a topic, connect to the TODO:LINK database and subscribe to the topic.

{% list tabs %}

- Go

   ```go
   reader, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"))
   if err != nil {
       return err
   }
   ```

{% endlist %}

To read messages from multiple topics or set more specific read options, you can use advanced settings for creating a consumer.

{% list tabs %}

- Go

   ```go
   reader, err := db.Topic().StartReader("consumer", []topicoptions.ReadSelector{
       {
           Path: "test",
       },
       {
           Path:       "test-2",
           Partitions: []int64{1, 2, 3},
           ReadFrom:   time.Date(2022, 7, 1, 10, 15, 0, 0, time.UTC),
       },
       },
   )
   if err != nil {
       return err
   }
   ```

{% endlist %}


### Message reads

The order of messages is guaranteed by the server within a single partition. The same guarantees apply to the SDK: messages from the same partition will be ordered among each other. Messages from different partitions may arrive in an order that differs from that they were written to the server and that the server passed them to the client in. However, the initial guarantees for ordering messages within a partition are valid through the entire message delivery path: from saving it to the partition to passing it to the client code.

Reading messages one by one:

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


Under batch message processing, it's more convenient to receive them in batches to make sure that messages within a batch get into the same partition.

{% list tabs %}

- Go

   ```go
   func ReadBatchesWithBatchCommit(ctx context.Context, r *topicreader.Reader) error {
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

### Message processing commits
The server can save the position of processed messages on its side if you send message processing commits to the server. This is an optional feature that often lets you simplify your code.

You can commit processing messages one by one:

{% list tabs %}

- Go

   ```go
   func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
       for {
           ...
           r.Commit(mess.Context(), mess)
       }
   }
   ```

{% endlist %}

and in batch mode

{% list tabs %}

- Go

   ```go
   func SimpleReadMessages(ctx context.Context, r *topicreader.Reader) error {
       for {
           ...
           r.Commit(batch.Context(), batch)
       }
   }
   ```

{% endlist %}

### Working with messages without committing their processing
To read messages without saving their progress in a topic, you should save it on your side and handle service messages about starting partition reads to let the server know the point in time to continue message delivery from. Without handling messages this way, the server will send you all available messages every time.

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

       r, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
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

### Selecting partitions
YDB uses server-side partition balancing across the clients connected, so the server may decide to stop sending messages to the client from some partitions. In this case, the client must terminate processing the messages it received.

The server has two ways to block a partition: a soft one (by notifying the client in advance) and a hard one (by sending a message saying that the partition can no longer be used).

The soft option means that the server has stopped delivering messages from this partition and will no longer send messages to it, but the client still has time to finish processing the messages received.

Handling the soft partition blocking method
{% list tabs %}

- Go

   In general, the SDK API doesn't send you an individual notification of softly blocking a partition.
   The SDK processes a signal so that a user immediately gets the messages remaining in the buffer even if
   the settings specify that a larger batch should be collected.

   ```go
   r, _ := db.Topic().StartReader("consumer", nil,
       topicoptions.WithBatchReadMinCount(1000),
   )

   for {
       batch, _ := r.ReadMessageBatch(ctx) // <- if partition soft stop batch can be less, then 1000
       processBatch(batch)
       _ = r.Commit(batch.Context(), batch)
   }

   ```
{% endlist %}

The hard option means that the client has to stop processing the messages received, because all non-committed messages will be passed to a different reader.

Handling the hard partition blocking method.
{% list tabs %}

- Go

   Each message (and each batch in case of batch reads) has a message context. If a partition is no longer available, batches and messages
   from this partition will have their context cancelled.

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
