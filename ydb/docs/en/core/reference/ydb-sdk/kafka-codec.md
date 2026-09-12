# Batch codecs

Usually every topic message is compressed separately with one of the [message codecs](../../concepts/datamodel/topic.md#message-codec). A *batch codec* works differently: a whole batch of messages is encoded into a single binary blob, which {{ ydb-short-name }} stores and transfers as is. Messages are extracted from such a blob either by the client (on read through the [Topic API](topic.md)) or by the server, when the same data has to be given out message by message.

A batch codec is selected by the writer and is sent in the `batch_codec` field of `StreamWriteMessage.WriteRequest`; the encoded batch itself is sent in `StreamWriteMessage.WriteRequest.EncodedBatch`. The list of batch codecs allowed for a topic is returned by the server in `StreamWriteMessage.InitResponse.supported_message_batch_codecs`. The available values are listed in the `MessagesBatchCodec` enum of [ydb_topic.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_topic.proto).

| Batch codec | Description |
| --- | --- |
| `MESSAGE_BATCH_CODEC_RAW` | No batch encoding: messages are passed as a regular list of protobuf messages, each one compressed with its own message codec. |
| `MESSAGE_BATCH_CODEC_KAFKA_BATCH` | The batch is encoded as an Apache Kafka record batch, see [Kafka batch](#kafka-batch). |

## Kafka batch {#kafka-batch}

With this codec, the batch payload is a binary Apache Kafka record batch of version 2 (`magic = 2`) — exactly the format that Kafka clients send in a `Produce` request and receive in a `Fetch` response. Storing data in this form lets {{ ydb-short-name }} pass messages between the [Kafka API](../kafka-api/index.md) and the Topic API without re-encoding them.

### Record batch format {#record-batch-format}

The following is the on-disk format of a `RecordBatch`:

```text
baseOffset: int64
batchLength: int32
partitionLeaderEpoch: int32
magic: int8 (current magic value is 2)
crc: uint32
attributes: int16
    bit 0~2:
        0: no compression
        1: gzip
        2: snappy
        3: lz4
        4: zstd
    bit 3: timestampType
    bit 4: isTransactional (0 means not transactional)
    bit 5: isControlBatch (0 means not a control batch)
    bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
    bit 7~15: unused
lastOffsetDelta: int32
baseTimestamp: int64
maxTimestamp: int64
producerId: int64
producerEpoch: int16
baseSequence: int32
recordsCount: int32
records: [Record]
```

When compression is enabled, the compressed record data is serialized directly following the count of the number of records.

`batchLength` represents the number of bytes from the current position (immediately after the `batchLength` field) to the end of the batch. In other words, the total size of a record batch is `batchLength + 12` bytes, which includes the 8-byte `baseOffset` and the 4-byte `batchLength` field itself.

The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC). It is located after the magic byte, which means that clients must parse the magic byte before deciding how to interpret the bytes between the batch length and the magic byte. The partition leader epoch field is not included in the CRC computation. The CRC-32C (Castagnoli) polynomial is used for the computation.

### Record format {#record-format}

The format of each record is delineated below:

```text
length: varint
attributes: int8
    bit 0~7: unused
timestampDelta: varlong
offsetDelta: varint
keyLength: varint
key: byte[]
valueLength: varint
value: byte[]
headersCount: varint
Headers => [Header]
```

Record header:

```text
headerKeyLength: varint
headerKey: String
headerValueLength: varint
Value: byte[]
```

The key of a record header is guaranteed to be non-null, while the value of a record header may be null. The order of headers in a record is preserved when producing and consuming. The varint encoding is the same as in [Protobuf](https://protobuf.dev/programming-guides/encoding/#varints). The count of headers in a record is also encoded as a varint.

{% note info %}

The format descriptions above are taken from the [Apache Kafka documentation](https://kafka.apache.org/documentation/#messageformat) (Apache License 2.0), which is the normative source of the format.

{% endnote %}

### Mapping to native topic messages {#mapping}

Below, the field names of the native message are given as they are declared in [ydb_topic.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_topic.proto).

Record batch fields:

| Kafka batch field | Native topic message |
| --- | --- |
| `baseOffset` | `offset` of the first message of the batch. On write it is ignored, the actual offsets are assigned by the server. |
| `batchLength`, `magic`, `crc` | Framing and integrity of the binary format, no counterpart. |
| `partitionLeaderEpoch` | No counterpart. |
| `attributes`, bits 0–2 | Codec of the message payloads inside the batch: `0` — `raw`, `1` — `gzip`, `4` — `zstd`. `snappy` and `lz4` have no counterpart among {{ ydb-short-name }} message codecs. |
| `attributes`, bit 3 (`timestampType`) | No counterpart, `created_at` always holds the timestamp provided by the writer. |
| `attributes`, bit 4 (`isTransactional`) | No counterpart, [topic transactions](../../concepts/datamodel/topic.md#topic-transactions) are expressed by the `tx` field of `StreamWriteMessage.WriteRequest`. |
| `attributes`, bit 5 (`isControlBatch`) | No counterpart, control batches are not exposed as topic messages. |
| `lastOffsetDelta` | `EncodedBatch.messages_count - 1`. |
| `baseTimestamp` | `created_at` of the first message of the batch. |
| `maxTimestamp` | The largest `created_at` among the messages of the batch. |
| `producerId`, `producerEpoch` | No counterpart, the writer is identified by the `producer_id` of the write session. Written as `0`. |
| `baseSequence` | `EncodedBatch.min_seq_no` — the `seq_no` of the first message of the batch. |
| `recordsCount` | `EncodedBatch.messages_count`. |
| `records` | The messages of the batch themselves. |

Record fields:

| Kafka record field | Native topic message |
| --- | --- |
| `length`, `attributes` | Framing of the binary format, no counterpart. |
| `timestampDelta` | `created_at` = `baseTimestamp` + `timestampDelta`, in milliseconds. |
| `offsetDelta` | `offset` = `baseOffset` + `offsetDelta`. |
| `key` | Message key, `partition_key`. |
| `value` | `data` — the message payload. |
| `headers` | `metadata_items` — the message metadata. |

The `seq_no` of a message is not stored in the record explicitly and is restored from the batch:

* if `producerId >= 0` — `seq_no` = (`baseSequence` + index of the record in the batch) mod 2<sup>31</sup>;
* otherwise — `seq_no` = `baseOffset` + `offsetDelta`.

### Example {#example}

A writer sends three messages in one batch to a topic through a write session with `producer_id = "my-producer"`:

| `seq_no` | `created_at` | `data` | Key |
| --- | --- | --- | --- |
| 10 | `2025-01-01T00:00:00.000Z` (`1735689600000` ms) | `msg-1` | `k1` |
| 11 | `2025-01-01T00:00:00.150Z` (`1735689600150` ms) | `msg-2` | — |
| 12 | `2025-01-01T00:00:00.400Z` (`1735689600400` ms) | `msg-3` | `k3` |

With the `MESSAGE_BATCH_CODEC_KAFKA_BATCH` batch codec and no compression of the payloads, this turns into one record batch:

```text
baseOffset            = 0                  // ignored on write, will be assigned by the server
magic                 = 2
attributes            = 0                  // bits 0~2 = 0: payloads are not compressed
lastOffsetDelta       = 2                  // 3 messages
baseTimestamp         = 1735689600000      // created_at of the first message
maxTimestamp          = 1735689600400      // created_at of the last message
producerId            = 0
producerEpoch         = 0
baseSequence          = 10                 // seq_no of the first message
recordsCount          = 3
records:
  [0] offsetDelta = 0, timestampDelta =   0, key = "k1",  value = "msg-1"
  [1] offsetDelta = 1, timestampDelta = 150, key = null,  value = "msg-2"
  [2] offsetDelta = 2, timestampDelta = 400, key = "k3",  value = "msg-3"
```

The serialized batch is sent in `StreamWriteMessage.WriteRequest`:

```text
batch_codec = MESSAGE_BATCH_CODEC_KAFKA_BATCH
batch_data:
  data              = <serialized record batch>
  messages_count    = 3
  min_seq_no        = 10
  max_seq_no        = 12
  uncompressed_size = <total size of the message payloads>
```

Assume the server has put the batch to the partition starting at offset `1000`. When such a batch is decoded back into messages, they are restored as follows:

| `offset` | `seq_no` | `created_at` | `data` | Key |
| --- | --- | --- | --- | --- |
| `1000` = 1000 + 0 | `10` = 10 + 0 | `1735689600000` + 0 | `msg-1` | `k1` |
| `1001` = 1000 + 1 | `11` = 10 + 1 | `1735689600000` + 150 | `msg-2` | — |
| `1002` = 1000 + 2 | `12` = 10 + 2 | `1735689600000` + 400 | `msg-3` | `k3` |
