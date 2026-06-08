#include <ydb/library/kafka/kafka_messages_int.h>
#include <ydb/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKafka {
namespace {

static constexpr size_t BUFFER_SIZE = 1 << 16;

template<class T>
void CheckUnsignedVarint(const std::vector<T>& values)  {
    for (T v : values) {
        Cerr << ">>>>> Check value=" << v << Endl << Flush;
        TKafkaWriteBuffer sb(BUFFER_SIZE);
        TKafkaWritable writable(sb);
        TKafkaReadable readable(sb.GetFrontBuffer());

        writable.writeUnsignedVarint(v);

        UNIT_ASSERT_EQUAL_C(sb.GetFrontBuffer().size(), NKafka::NPrivate::SizeOfUnsignedVarint<T>(v),
            TStringBuilder() << "Size mismatch " << sb.GetFrontBuffer().size() << " != " << NKafka::NPrivate::SizeOfUnsignedVarint<T>(v));

        T r = readable.readUnsignedVarint<T>();
        UNIT_ASSERT_EQUAL_C(r, v, TStringBuilder() << r << " != " << v);
    }
}

template<class T>
void CheckVarint(const std::vector<T>& values) {
    for (T v : values) {
        Cerr << ">>>>> Check value=" << v << Endl << Flush;
        TKafkaWriteBuffer sb(BUFFER_SIZE);
        TKafkaWritable writable(sb);
        TKafkaReadable readable(sb.GetFrontBuffer());

        writable.writeVarint(v);

        UNIT_ASSERT_EQUAL_C(sb.GetFrontBuffer().size(), NKafka::NPrivate::SizeOfVarint<T>(v),
            TStringBuilder() << "Size mismatch " << sb.GetFrontBuffer().size() << " != " << NKafka::NPrivate::SizeOfVarint<T>(v));

        T r = readable.readVarint<T>();

        UNIT_ASSERT_EQUAL_C(r, v, TStringBuilder() << r << " != " << v);
    }
}

template<class T>
void CheckVarintWrongBytes(std::vector<ui8> bytes) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb.GetFrontBuffer());

    writable.write(reinterpret_cast<char*>(bytes.data()), bytes.size());

    try {
        readable.readUnsignedVarint<T>();
        UNIT_FAIL("Must be exception");
    } catch (const yexception& e) {
        UNIT_ASSERT_STRING_CONTAINS(e.what(), "illegal varint length");
    }
}

TKafkaRecord MakeRecord(i64 timestampDelta, i64 offsetDelta, TStringBuf key, TStringBuf value) {
    TKafkaRecord record;
    record.TimestampDelta = timestampDelta;
    record.OffsetDelta = offsetDelta;
    record.Key = key;
    record.Value = value;
    record.Length = record.Size(2) - NKafka::NPrivate::SizeOfVarint<TKafkaRecord::LengthMeta::Type>(0);
    return record;
}

TKafkaRecordBatch MakeRecordBatch(ECompressionType compressionType) {
    TKafkaRecordBatch batch;
    batch.BaseOffset = 42;
    batch.Magic = 2;
    batch.Attributes = static_cast<TKafkaRecordBatch::AttributesMeta::Type>(compressionType);
    batch.LastOffsetDelta = 1;
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1010;
    batch.Records.push_back(MakeRecord(7, 0, "key-0", "value-0"));
    batch.Records.push_back(MakeRecord(10, 1, "key-1", "value-1"));
    batch.BatchLength = batch.Size(2) - sizeof(TKafkaRecordBatch::BaseOffsetMeta::Type) - sizeof(TKafkaRecordBatch::BatchLengthMeta::Type);
    return batch;
}

void AssertRecordBatchRoundTrip(ECompressionType compressionType) {
    const TKafkaRecordBatch batch = MakeRecordBatch(compressionType);

    const TString serialized = WriteKafkaRecordBatch(batch);
    const TKafkaRecordBatch parsed = ReadKafkaRecordBatch(serialized);

    UNIT_ASSERT_VALUES_EQUAL(parsed.BaseOffset, batch.BaseOffset);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Magic, batch.Magic);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(parsed.CompressionType()), static_cast<int>(compressionType));
    UNIT_ASSERT_VALUES_EQUAL(parsed.BatchLength, batch.BatchLength);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Records.size(), batch.Records.size());
    for (size_t i = 0; i < batch.Records.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(parsed.Records[i].TimestampDelta, batch.Records[i].TimestampDelta);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Records[i].OffsetDelta, batch.Records[i].OffsetDelta);
        UNIT_ASSERT_VALUES_EQUAL(*parsed.Records[i].Key, *batch.Records[i].Key);
        UNIT_ASSERT_VALUES_EQUAL(*parsed.Records[i].Value, *batch.Records[i].Value);
    }
}

void AssertUnsupportedCompressionType(ECompressionType compressionType) {
    TKafkaRecordBatch batch = MakeRecordBatch(ECompressionType::NONE);
    batch.Attributes = static_cast<TKafkaRecordBatch::AttributesMeta::Type>(compressionType);

    try {
        Y_UNUSED(batch.Size(2));
        UNIT_FAIL("Must be exception");
    } catch (const yexception& e) {
        UNIT_ASSERT_STRING_CONTAINS(e.what(), "unsupported Kafka record batch compression type");
    }
}

void AssertSkipDecompressionRead(ECompressionType compressionType) {
    const TKafkaRecordBatch batch = MakeRecordBatch(compressionType);
    const TString serialized = WriteKafkaRecordBatch(batch);

    TBuffer buffer(serialized.data(), serialized.size());
    TKafkaReadable readable(buffer);

    TKafkaRecordBatch parsed;
    parsed.Read(readable, 2, {.AllowCompressed = true, .SkipDecompression = true});

    UNIT_ASSERT(parsed.Records.empty());
    UNIT_ASSERT(!parsed.PackedRecords.empty());
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(parsed.CompressionType()), static_cast<int>(compressionType));

    const TString roundTrip = WriteKafkaRecordBatch(parsed);
    UNIT_ASSERT_VALUES_EQUAL(roundTrip, serialized);
}

TString Bytes(std::initializer_list<ui8> bytes) {
    TString result;
    result.reserve(bytes.size());
    for (const ui8 byte : bytes) {
        result.push_back(static_cast<char>(byte));
    }
    return result;
}

// Generated from Apache Kafka commit 47fbf15f7b:
//
// for (CompressionType type : new CompressionType[]{CompressionType.NONE, CompressionType.GZIP, CompressionType.ZSTD}) {
//     MemoryRecordsBuilder builder = MemoryRecords.builder(
//         ByteBuffer.allocate(1024), RecordBatch.MAGIC_VALUE_V2, Compression.of(type).build(),
//         TimestampType.CREATE_TIME, 42L, RecordBatch.NO_TIMESTAMP);
//     ProducerBatch batch = new ProducerBatch(new TopicPartition("topic", 0), builder, 1000L);
//     batch.tryAppend(1007L, "key-0".getBytes(), "value-0".getBytes(),
//         new Header[]{new RecordHeader("header-0", "hvalue-0".getBytes())}, null, 1007L);
//     batch.tryAppend(1010L, "key-1".getBytes(), "value-1".getBytes(),
//         new Header[]{new RecordHeader("header-1", "hvalue-1".getBytes())}, null, 1010L);
//     ByteBuffer buffer = batch.records().buffer();
//     byte[] bytes = new byte[buffer.remaining()];
//     buffer.get(bytes);
//     System.out.println(type + " " + toCppBytes(bytes));
// }
TString KafkaProducerBatchBytes(ECompressionType compressionType) {
    switch (compressionType) {
        case ECompressionType::NONE:
            return Bytes({
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x7B, 0xFF, 0xFF, 0xFF, 0xFF,
                0x02, 0xF6, 0x09, 0xC5, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x03, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xF2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x02, 0x48, 0x00, 0x00,
                0x00, 0x0A, 0x6B, 0x65, 0x79, 0x2D, 0x30, 0x0E, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x2D, 0x30, 0x02,
                0x10, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2D, 0x30, 0x10, 0x68, 0x76, 0x61, 0x6C, 0x75, 0x65,
                0x2D, 0x30, 0x48, 0x00, 0x06, 0x02, 0x0A, 0x6B, 0x65, 0x79, 0x2D, 0x31, 0x0E, 0x76, 0x61, 0x6C,
                0x75, 0x65, 0x2D, 0x31, 0x02, 0x10, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2D, 0x31, 0x10, 0x68,
                0x76, 0x61, 0x6C, 0x75, 0x65, 0x2D, 0x31,
            });
        case ECompressionType::GZIP:
            return Bytes({
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x75, 0xFF, 0xFF, 0xFF, 0xFF,
                0x02, 0x29, 0x52, 0xC6, 0x73, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x03, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xF2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x02, 0x1F, 0x8B, 0x08,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xF3, 0x60, 0x60, 0x60, 0xE0, 0xCA, 0x4E, 0xAD, 0xD4,
                0x35, 0xE0, 0x2B, 0x4B, 0xCC, 0x29, 0x4D, 0xD5, 0x35, 0x60, 0x12, 0xC8, 0x48, 0x4D, 0x4C, 0x49,
                0x2D, 0xD2, 0x35, 0x10, 0xC8, 0x80, 0x0A, 0x79, 0x30, 0xB0, 0x31, 0x81, 0x15, 0x19, 0x42, 0x15,
                0x19, 0xC2, 0x15, 0x19, 0xC2, 0x14, 0x19, 0x02, 0x00, 0xB1, 0x38, 0x5C, 0x0F, 0x4A, 0x00, 0x00,
                0x00,
            });
        case ECompressionType::ZSTD:
            return Bytes({
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x72, 0xFF, 0xFF, 0xFF, 0xFF,
                0x02, 0xD6, 0xD3, 0xEA, 0xF9, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x03, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xF2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x02, 0x28, 0xB5, 0x2F,
                0xFD, 0x00, 0x58, 0xAC, 0x01, 0x00, 0x64, 0x02, 0x48, 0x00, 0x00, 0x00, 0x0A, 0x6B, 0x65, 0x79,
                0x2D, 0x30, 0x0E, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x2D, 0x30, 0x02, 0x10, 0x68, 0x65, 0x61, 0x64,
                0x65, 0x72, 0x2D, 0x30, 0x10, 0x68, 0x48, 0x00, 0x06, 0x02, 0x31, 0x31, 0x31, 0x31, 0x05, 0x00,
                0x80, 0x08, 0x28, 0x07, 0x92, 0x21, 0xD5, 0xB2, 0xE6, 0x4E, 0x27, 0x01, 0x00, 0x00,
            });
        default:
            ythrow yexception() << "unexpected golden Kafka record batch compression type: " << static_cast<int>(compressionType);
    }
}

void AssertKafkaRecord(
    const TKafkaRecord& record,
    i64 timestampDelta,
    i64 offsetDelta,
    TStringBuf key,
    TStringBuf value,
    TStringBuf headerKey,
    TStringBuf headerValue)
{
    UNIT_ASSERT_VALUES_EQUAL(record.TimestampDelta, timestampDelta);
    UNIT_ASSERT_VALUES_EQUAL(record.OffsetDelta, offsetDelta);
    UNIT_ASSERT(record.Key);
    UNIT_ASSERT(record.Value);
    UNIT_ASSERT_VALUES_EQUAL(*record.Key, key);
    UNIT_ASSERT_VALUES_EQUAL(*record.Value, value);
    UNIT_ASSERT_VALUES_EQUAL(record.Headers.size(), 1);
    UNIT_ASSERT(record.Headers[0].Key);
    UNIT_ASSERT(record.Headers[0].Value);
    UNIT_ASSERT_VALUES_EQUAL(*record.Headers[0].Key, headerKey);
    UNIT_ASSERT_VALUES_EQUAL(*record.Headers[0].Value, headerValue);
}

void AssertKafkaProducerBatchDeserialized(ECompressionType compressionType) {
    const TKafkaRecordBatch parsed = ReadKafkaRecordBatch(KafkaProducerBatchBytes(compressionType));

    UNIT_ASSERT_VALUES_EQUAL(parsed.BaseOffset, 42);
    UNIT_ASSERT_VALUES_EQUAL(parsed.PartitionLeaderEpoch, -1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Magic, 2);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(parsed.CompressionType()), static_cast<int>(compressionType));
    UNIT_ASSERT_VALUES_EQUAL(parsed.LastOffsetDelta, 1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.BaseTimestamp, 1007);
    UNIT_ASSERT_VALUES_EQUAL(parsed.MaxTimestamp, 1010);
    UNIT_ASSERT_VALUES_EQUAL(parsed.ProducerId, -1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.ProducerEpoch, -1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.BaseSequence, -1);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Records.size(), 2);

    AssertKafkaRecord(parsed.Records[0], 0, 0, "key-0", "value-0", "header-0", "hvalue-0");
    AssertKafkaRecord(parsed.Records[1], 3, 1, "key-1", "value-1", "header-1", "hvalue-1");
}

void AssertKafkaProducerBatchSerialized(ECompressionType compressionType) {
    const TString serialized = KafkaProducerBatchBytes(compressionType);
    TKafkaRecordBatch parsed = ReadKafkaRecordBatch(
        serialized,
        2,
        {.AllowCompressed = true, .SkipDecompression = compressionType != ECompressionType::NONE});

    if (compressionType != ECompressionType::NONE) {
        UNIT_ASSERT(parsed.Records.empty());
        UNIT_ASSERT(!parsed.PackedRecords.empty());
    }

    UNIT_ASSERT_VALUES_EQUAL(WriteKafkaRecordBatch(parsed), serialized);
}

Y_UNIT_TEST_SUITE(KafkaRecords) {
    Y_UNIT_TEST(UnsignedVarint32) {
        CheckUnsignedVarint<ui32>({0, 1, 127, 128, 32191, Max<i32>(), Max<ui32>()});
    }

    Y_UNIT_TEST(UnsignedVarint64) {
        CheckUnsignedVarint<ui64>({0, 1, 127, 128, 32191, Max<i32>(), static_cast<unsigned long>(Max<i32>()) + 1, Max<i64>(), Max<ui64>()});
    }

    Y_UNIT_TEST(Varint32) {
        CheckVarint<i32>({Min<i32>(), -167966, -1, 0, 1, 127, 128, 32191, Max<i32>()});
    }

    Y_UNIT_TEST(Varint64) {
        CheckVarint<i64>({Min<i64>(), Min<i32>(), -167966, -1, 0, 1, 127, 128, 32191, static_cast<unsigned long>(Max<i32>()) + 1, Max<i64>()});
    }

    Y_UNIT_TEST(UnsignedVarint32Wrong) {
        CheckVarintWrongBytes<ui32>({0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
    }

    Y_UNIT_TEST(UnsignedVarint64Wrong) {
        CheckVarintWrongBytes<ui64>({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
    }

    Y_UNIT_TEST(UnsignedVarint32Deserialize) {
        std::vector<ui8> bytes = {0x81, 0x83, 0x05};

        TKafkaWriteBuffer sb(BUFFER_SIZE);
        TKafkaWritable writable(sb);
        TKafkaReadable readable(sb.GetFrontBuffer());

        writable.write(reinterpret_cast<char*>(bytes.data()), bytes.size());

        ui32 result = readable.readUnsignedVarint<ui32>();
        UNIT_ASSERT_EQUAL(result, 1 + (3 << 7) + (5 << 14));
    }

    Y_UNIT_TEST(RecordBatchRoundTrip) {
        AssertRecordBatchRoundTrip(ECompressionType::NONE);
    }

    Y_UNIT_TEST(RecordBatchGzipRoundTrip) {
        AssertRecordBatchRoundTrip(ECompressionType::GZIP);
    }

    Y_UNIT_TEST(RecordBatchZstdRoundTrip) {
        AssertRecordBatchRoundTrip(ECompressionType::ZSTD);
    }

    Y_UNIT_TEST(RecordBatchGzipSkipDecompression) {
        AssertSkipDecompressionRead(ECompressionType::GZIP);
    }

    Y_UNIT_TEST(RecordBatchZstdSkipDecompression) {
        AssertSkipDecompressionRead(ECompressionType::ZSTD);
    }

    Y_UNIT_TEST(KafkaProducerRecordBatchDeserialize) {
        AssertKafkaProducerBatchDeserialized(ECompressionType::NONE);
        AssertKafkaProducerBatchDeserialized(ECompressionType::GZIP);
        AssertKafkaProducerBatchDeserialized(ECompressionType::ZSTD);
    }

    Y_UNIT_TEST(KafkaProducerRecordBatchSerialize) {
        AssertKafkaProducerBatchSerialized(ECompressionType::NONE);
        AssertKafkaProducerBatchSerialized(ECompressionType::GZIP);
        AssertKafkaProducerBatchSerialized(ECompressionType::ZSTD);
    }

    Y_UNIT_TEST(RecordBatchUnsupportedCompressionType) {
        AssertUnsupportedCompressionType(ECompressionType::SNAPPY);
        AssertUnsupportedCompressionType(ECompressionType::LZ4);
    }
}

} // namespace
} // namespace NKafka
