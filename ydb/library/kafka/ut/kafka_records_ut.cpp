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
        TKafkaRecord record;
        record.Length = 10;
        record.TimestampDelta = 7;
        record.OffsetDelta = 0;
        record.Key = TKafkaRawBytes("key", 3);
        record.Value = TKafkaRawBytes("value", 5);

        TKafkaRecordBatch batch;
        batch.BaseOffset = 42;
        batch.BatchLength = batch.Size(2) - sizeof(TKafkaRecordBatch::BaseOffsetMeta::Type) - sizeof(TKafkaRecordBatch::BatchLengthMeta::Type);
        batch.Magic = 2;
        batch.LastOffsetDelta = 0;
        batch.BaseTimestamp = 1000;
        batch.MaxTimestamp = 1007;
        batch.Records.push_back(record);

        const TString serialized = WriteKafkaRecordBatch(batch);
        const TKafkaRecordBatch parsed = ReadKafkaRecordBatch(serialized);

        UNIT_ASSERT_VALUES_EQUAL(parsed.BaseOffset, batch.BaseOffset);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Magic, batch.Magic);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Records.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Records[0].TimestampDelta, record.TimestampDelta);
        UNIT_ASSERT_VALUES_EQUAL(TString(parsed.Records[0].Key->data(), parsed.Records[0].Key->size()), "key");
        UNIT_ASSERT_VALUES_EQUAL(TString(parsed.Records[0].Value->data(), parsed.Records[0].Value->size()), "value");
    }
}

} // namespace
} // namespace NKafka
