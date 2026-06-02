#include <ydb/core/persqueue/public/kafka_batch/kafka_batch.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NKikimr::NPQ::NKafkaBatch {
namespace {

void WriteInt8(TString& out, i8 value) {
    out.push_back(static_cast<char>(value));
}

void WriteInt16(TString& out, i16 value) {
    out.push_back(static_cast<char>((value >> 8) & 0xff));
    out.push_back(static_cast<char>(value & 0xff));
}

void WriteInt32(TString& out, i32 value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

void WriteInt64(TString& out, i64 value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

template <typename S, typename U = std::make_unsigned_t<S>>
void WriteVarInt(TString& out, S signedValue) {
    U value = (static_cast<U>(signedValue) << 1) ^ static_cast<U>(signedValue >> (sizeof(S) * 8 - 1));
    while ((value & ~static_cast<U>(0x7f)) != 0) {
        out.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

void WriteVarBytes(TString& out, TStringBuf value) {
    WriteVarInt<i32>(out, value.size());
    out.append(value.data(), value.size());
}

void WriteRecord(TString& out, i64 timestampDelta, i64 offsetDelta, TStringBuf key, TStringBuf value) {
    TString body;
    WriteInt8(body, 0); // attributes
    WriteVarInt<i64>(body, timestampDelta);
    WriteVarInt<i64>(body, offsetDelta);
    WriteVarBytes(body, key);
    WriteVarBytes(body, value);
    WriteVarInt<i32>(body, 1); // headers count
    WriteVarBytes(body, "h");
    WriteVarBytes(body, TStringBuilder() << "v" << offsetDelta);

    WriteVarInt<i32>(out, body.size());
    out += body;
}

TString MakeBatch() {
    TString records;
    WriteInt32(records, 2); // records count
    WriteRecord(records, 5, 0, "k0", "value0");
    WriteRecord(records, 7, 1, "k1", "value1");

    TString body;
    WriteInt32(body, -1); // partitionLeaderEpoch
    WriteInt8(body, 2); // magic
    WriteInt32(body, 0); // crc, currently not validated by parser
    WriteInt16(body, 0); // attributes: no compression
    WriteInt32(body, 1); // lastOffsetDelta
    WriteInt64(body, 1000); // baseTimestamp
    WriteInt64(body, 1007); // maxTimestamp
    WriteInt64(body, 42); // producerId
    WriteInt16(body, 3); // producerEpoch
    WriteInt32(body, 10); // baseSequence
    body += records;

    TString batch;
    WriteInt64(batch, 100); // baseOffset
    WriteInt32(batch, body.size());
    batch += body;
    return batch;
}

} // namespace

Y_UNIT_TEST_SUITE(TKafkaBatchParserTest) {
    Y_UNIT_TEST(ParseUncompressedRecordBatchV2) {
        const auto batch = ParseKafkaBatch(MakeBatch());

        UNIT_ASSERT_VALUES_EQUAL(batch.BaseOffset, 100u);
        UNIT_ASSERT_VALUES_EQUAL(batch.LastOffsetDelta, 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.BaseTimestamp, 1000);
        UNIT_ASSERT_VALUES_EQUAL(batch.MaxTimestamp, 1007);
        UNIT_ASSERT_VALUES_EQUAL(batch.ProducerId, 42);
        UNIT_ASSERT_VALUES_EQUAL(batch.ProducerEpoch, 3);
        UNIT_ASSERT_VALUES_EQUAL(batch.BaseSequence, 10);
        UNIT_ASSERT_VALUES_EQUAL(batch.Records.size(), 2u);

        UNIT_ASSERT_VALUES_EQUAL(batch.Records[0].Offset, 100u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Records[0].Sequence, 10u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Records[0].Timestamp, 1005);
        UNIT_ASSERT(batch.Records[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(*batch.Records[0].Key, "k0");
        UNIT_ASSERT(batch.Records[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(*batch.Records[0].Value, "value0");
        UNIT_ASSERT_VALUES_EQUAL(batch.Records[0].Headers.size(), 1u);
        UNIT_ASSERT(batch.Records[0].Headers[0].Key);
        UNIT_ASSERT_VALUES_EQUAL(*batch.Records[0].Headers[0].Key, "h");
        UNIT_ASSERT(batch.Records[0].Headers[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(*batch.Records[0].Headers[0].Value, "v0");

        UNIT_ASSERT_VALUES_EQUAL(batch.Records[1].Offset, 101u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Records[1].Sequence, 11u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Records[1].Timestamp, 1007);
    }
}

} // namespace NKikimr::NPQ::NKafkaBatch
