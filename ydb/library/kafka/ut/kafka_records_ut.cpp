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
        TWritableBuf sb(BUFFER_SIZE);
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
        TWritableBuf sb(BUFFER_SIZE);
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
    TWritableBuf sb(BUFFER_SIZE);
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
    record.Key = TKafkaRawBytes(key.data(), key.size());
    record.Value = TKafkaRawBytes(value.data(), value.size());
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
        UNIT_ASSERT_VALUES_EQUAL(
            TString(parsed.Records[i].Key->data(), parsed.Records[i].Key->size()),
            TString(batch.Records[i].Key->data(), batch.Records[i].Key->size()));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(parsed.Records[i].Value->data(), parsed.Records[i].Value->size()),
            TString(batch.Records[i].Value->data(), batch.Records[i].Value->size()));
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

        TWritableBuf sb(BUFFER_SIZE);
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

    Y_UNIT_TEST(RecordBatchUnsupportedCompressionType) {
        AssertUnsupportedCompressionType(ECompressionType::SNAPPY);
        AssertUnsupportedCompressionType(ECompressionType::LZ4);
    }
}

} // namespace
} // namespace NKafka
