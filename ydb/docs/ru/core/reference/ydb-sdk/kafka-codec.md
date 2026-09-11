# Батчевые кодеки

Обычно каждое сообщение топика сжимается отдельно одним из [кодеков сообщений](../../concepts/datamodel/topic.md#message-codec). *Батчевый кодек* работает иначе: в единый бинарный блок кодируется целый пакет сообщений, который {{ ydb-short-name }} хранит и передаёт как есть. Сообщения извлекаются из такого блока либо клиентом (при чтении через [Topic API](topic.md)), либо сервером, если те же данные нужно выдать по одному сообщению.

Батчевый кодек выбирается писателем и передаётся в поле `batch_codec` сообщения `StreamWriteMessage.WriteRequest`, а сам закодированный пакет — в `StreamWriteMessage.WriteRequest.EncodedBatch`. Список батчевых кодеков, разрешённых для топика, сервер возвращает в `StreamWriteMessage.InitResponse.supported_message_batch_codecs`. Доступные значения перечислены в enum `MessagesBatchCodec` в [ydb_topic.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_topic.proto).

| Батчевый кодек | Описание |
| --- | --- |
| `MESSAGE_BATCH_CODEC_RAW` | Без батчевого кодирования: сообщения передаются обычным списком protobuf-сообщений, каждое сжато своим кодеком сообщения. |
| `MESSAGE_BATCH_CODEC_KAFKA_BATCH` | Пакет закодирован как record batch Apache Kafka, см. [Kafka batch](#kafka-batch). |

## Kafka batch {#kafka-batch}

При использовании этого кодека содержимое пакета — бинарный Apache Kafka record batch версии 2 (`magic = 2`), ровно тот формат, который Kafka-клиенты отправляют в запросе `Produce` и получают в ответе `Fetch`. Хранение данных в таком виде позволяет {{ ydb-short-name }} передавать сообщения между [Kafka API](../kafka-api/index.md) и Topic API без перекодирования.

### Формат record batch {#record-batch-format}

Формат `RecordBatch` в бинарном представлении:

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

Если включено сжатие, сжатые данные записей сериализуются сразу за количеством записей.

`batchLength` — количество байт от текущей позиции (сразу после поля `batchLength`) до конца пакета. То есть полный размер record batch равен `batchLength + 12` байт, включая 8-байтовый `baseOffset` и 4-байтовое поле `batchLength`.

CRC покрывает данные от `attributes` до конца пакета (то есть все байты после самого CRC). CRC расположен после байта `magic`, поэтому клиент должен сначала разобрать `magic` и только потом решать, как интерпретировать байты между длиной пакета и `magic`. Поле `partitionLeaderEpoch` в вычисление CRC не входит. Используется полином CRC-32C (Castagnoli).

### Формат record {#record-format}

Формат каждой записи:

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

Заголовок записи (record header):

```text
headerKeyLength: varint
headerKey: String
headerValueLength: varint
Value: byte[]
```

Ключ заголовка записи гарантированно не `null`, значение может быть `null`. Порядок заголовков внутри записи сохраняется при записи и чтении. Используется то же varint-кодирование, что и в [Protobuf](https://protobuf.dev/programming-guides/encoding/#varints). Количество заголовков в записи также закодировано как varint.

{% note info %}

Описания форматов выше взяты из [документации Apache Kafka](https://kafka.apache.org/documentation/#messageformat) (Apache License 2.0) — она является нормативным источником формата.

{% endnote %}

### Соответствие нативным сообщениям топика {#mapping}

Ниже поля нативного сообщения приведены так, как они объявлены в [ydb_topic.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_topic.proto).

Поля record batch:

| Поле Kafka-пакета | Нативное сообщение топика |
| --- | --- |
| `baseOffset` | `offset` первого сообщения пакета. При записи игнорируется, реальные offset'ы назначает сервер. |
| `batchLength`, `magic`, `crc` | Разметка и контроль целостности бинарного формата, соответствия нет. |
| `partitionLeaderEpoch` | Соответствия нет. |
| `attributes`, биты 0–2 | Кодек тел сообщений внутри пакета: `0` — `raw`, `1` — `gzip`, `4` — `zstd`. `snappy` и `lz4` не имеют соответствия среди кодеков сообщений {{ ydb-short-name }}. |
| `attributes`, бит 3 (`timestampType`) | Соответствия нет, в `created_at` всегда время, переданное писателем. |
| `attributes`, бит 4 (`isTransactional`) | Соответствия нет, [транзакции с топиками](../../concepts/datamodel/topic.md#topic-transactions) задаются полем `tx` в `StreamWriteMessage.WriteRequest`. |
| `attributes`, бит 5 (`isControlBatch`) | Соответствия нет, control-пакеты не выдаются как сообщения топика. |
| `lastOffsetDelta` | `EncodedBatch.messages_count - 1`. |
| `baseTimestamp` | `created_at` первого сообщения пакета. |
| `maxTimestamp` | Наибольшее `created_at` среди сообщений пакета. |
| `producerId`, `producerEpoch` | Соответствия нет, писатель идентифицируется полем `producer_id` сессии записи. Записывается значение `0`. |
| `baseSequence` | `EncodedBatch.min_seq_no` — `seq_no` первого сообщения пакета. |
| `recordsCount` | `EncodedBatch.messages_count`. |
| `records` | Сами сообщения пакета. |

Поля record:

| Поле Kafka-записи | Нативное сообщение топика |
| --- | --- |
| `length`, `attributes` | Разметка бинарного формата, соответствия нет. |
| `timestampDelta` | `created_at` = `baseTimestamp` + `timestampDelta`, в миллисекундах. |
| `offsetDelta` | `offset` = `baseOffset` + `offsetDelta`. |
| `key` | Ключ сообщения, `partition_key`. |
| `value` | `data` — тело сообщения. |
| `headers` | `metadata_items` — метаданные сообщения. |

`seq_no` сообщения в записи явно не хранится и восстанавливается из полей пакета:

* если `producerId >= 0` — `seq_no` = (`baseSequence` + индекс записи в пакете) mod 2<sup>31</sup>;
* иначе — `seq_no` = `baseOffset` + `offsetDelta`.

### Пример {#example}

Писатель отправляет одним пакетом три сообщения в топик через сессию записи с `producer_id = "my-producer"`:

| `seq_no` | `created_at` | `data` | Ключ |
| --- | --- | --- | --- |
| 10 | `2025-01-01T00:00:00.000Z` (`1735689600000` мс) | `msg-1` | `k1` |
| 11 | `2025-01-01T00:00:00.150Z` (`1735689600150` мс) | `msg-2` | — |
| 12 | `2025-01-01T00:00:00.400Z` (`1735689600400` мс) | `msg-3` | `k3` |

С батчевым кодеком `MESSAGE_BATCH_CODEC_KAFKA_BATCH` и без сжатия тел сообщений это превращается в один record batch:

```text
baseOffset            = 0                  // при записи игнорируется, будет назначен сервером
magic                 = 2
attributes            = 0                  // биты 0~2 = 0: тела сообщений не сжаты
lastOffsetDelta       = 2                  // 3 сообщения
baseTimestamp         = 1735689600000      // created_at первого сообщения
maxTimestamp          = 1735689600400      // created_at последнего сообщения
producerId            = 0
producerEpoch         = 0
baseSequence          = 10                 // seq_no первого сообщения
recordsCount          = 3
records:
  [0] offsetDelta = 0, timestampDelta =   0, key = "k1",  value = "msg-1"
  [1] offsetDelta = 1, timestampDelta = 150, key = null,  value = "msg-2"
  [2] offsetDelta = 2, timestampDelta = 400, key = "k3",  value = "msg-3"
```

Сериализованный пакет передаётся в `StreamWriteMessage.WriteRequest`:

```text
batch_codec = MESSAGE_BATCH_CODEC_KAFKA_BATCH
batch_data:
  data              = <сериализованный record batch>
  messages_count    = 3
  min_seq_no        = 10
  max_seq_no        = 12
  uncompressed_size = <суммарный размер тел сообщений>
```

Пусть сервер разместил пакет в партиции начиная с offset `1000`. При обратном разборе пакета на сообщения они восстановятся так:

| `offset` | `seq_no` | `created_at` | `data` | Ключ |
| --- | --- | --- | --- | --- |
| `1000` = 1000 + 0 | `10` = 10 + 0 | `1735689600000` + 0 | `msg-1` | `k1` |
| `1001` = 1000 + 1 | `11` = 10 + 1 | `1735689600000` + 150 | `msg-2` | — |
| `1002` = 1000 + 2 | `12` = 10 + 2 | `1735689600000` + 400 | `msg-3` | `k3` |
