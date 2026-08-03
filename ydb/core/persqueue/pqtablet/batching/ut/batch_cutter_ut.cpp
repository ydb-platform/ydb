#include <ydb/core/persqueue/pqtablet/batching/batch_cutter.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/string.h>
#include <util/stream/mem.h>
#include <util/stream/zlib.h>

namespace NKikimr::NPQ::NBatching {
namespace {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;

NPersQueueCommon::ECodec KafkaBatchCodec() {
    return static_cast<NPersQueueCommon::ECodec>(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1);
}

NKafka::TKafkaRecord MakeKafkaRecord(
    i64 timestampDelta,
    i64 offsetDelta,
    TStringBuf key,
    TStringBuf value)
{
    NKafka::TKafkaRecord record;
    record.TimestampDelta = timestampDelta;
    record.OffsetDelta = offsetDelta;
    record.SetKey(TString{key});
    record.SetValue(TString{value});
    record.Length = record.Size(2)
        - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
    return record;
}

NKafka::TKafkaRecord MakeKafkaRecordWithoutKey(
    i64 timestampDelta,
    i64 offsetDelta,
    TStringBuf value)
{
    NKafka::TKafkaRecord record;
    record.TimestampDelta = timestampDelta;
    record.OffsetDelta = offsetDelta;
    record.SetValue(TString{value});
    record.Length = record.Size(2)
        - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
    return record;
}

TString MakeKafkaBatchPayload(NKafka::ECompressionType compression = NKafka::ECompressionType::NONE) {
    NKafka::TKafkaRecordBatch batch;
    batch.BaseOffset = 100;
    batch.Magic = 2;
    batch.Attributes = static_cast<NKafka::TKafkaRecordBatch::AttributesMeta::Type>(compression);
    batch.LastOffsetDelta = 1;
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1007;
    batch.ProducerId = 42;
    batch.ProducerEpoch = 3;
    batch.BaseSequence = 10;
    batch.Records.push_back(MakeKafkaRecord(5, 0, "k0", "value0"));
    batch.Records.push_back(MakeKafkaRecord(7, 1, "k1", "value1"));
    batch.BatchLength = batch.Size(2)
        - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type)
        - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
    return NKafka::WriteKafkaRecordBatch(batch);
}

TString MakeKafkaBatchPayloadWithNullKey() {
    NKafka::TKafkaRecordBatch batch;
    batch.BaseOffset = 100;
    batch.Magic = 2;
    batch.LastOffsetDelta = 1;
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1007;
    batch.ProducerId = 42;
    batch.ProducerEpoch = 3;
    batch.BaseSequence = 10;
    batch.Records.push_back(MakeKafkaRecordWithoutKey(5, 0, "value0"));
    batch.Records.push_back(MakeKafkaRecord(7, 1, "k1", "value1"));
    batch.BatchLength = batch.Size(2)
        - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type)
        - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
    return NKafka::WriteKafkaRecordBatch(batch);
}

TString SerializeDataChunk(NKikimrPQClient::TDataChunk chunk) {
    TString serialized;
    Y_ENSURE(chunk.SerializeToString(&serialized));
    return serialized;
}

TReadResult MakeKafkaBatchReadResult(
    TString payload,
    NPersQueueCommon::ECodec codec = KafkaBatchCodec(),
    ui64 uncompressedSize = 0,
    NKikimrPQClient::TDataChunk::EChunkType chunkType = NKikimrPQClient::TDataChunk::REGULAR,
    ui64 seqNo = 100)
{
    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(chunkType);
    chunk.SetCodec(codec);
    chunk.SetData(std::move(payload));
    chunk.SetSeqNo(seqNo);

    TReadResult readResult;
    readResult.SetOffset(10);
    readResult.SetSeqNo(seqNo);
    readResult.SetLogicalMessageCount(2);
    readResult.SetIsBatch(true);
    readResult.SetData(SerializeDataChunk(std::move(chunk)));
    readResult.SetWriteTimestampMS(777);
    if (uncompressedSize > 0) {
        readResult.SetUncompressedSize(uncompressedSize);
    }
    return readResult;
}

TString DecompressPayload(TStringBuf data, NPersQueueCommon::ECodec codec) {
    if (codec == NPersQueueCommon::RAW) {
        return TString(data);
    }

    TMemoryInput input(data.data(), data.size());
    switch (codec) {
        case NPersQueueCommon::GZIP: {
            TZLibDecompress gzip(&input, ZLib::GZip);
            return gzip.ReadAll();
        }
        case NPersQueueCommon::ZSTD: {
            TZstdDecompress zstd(&input);
            return zstd.ReadAll();
        }
        default:
            ythrow yexception() << "unsupported message codec: " << static_cast<int>(codec);
    }
}

void AssertKafkaBatchCut(
    const TVector<TReadResult>& cut,
    NPersQueueCommon::ECodec expectedCodec = NPersQueueCommon::RAW)
{
    UNIT_ASSERT_VALUES_EQUAL(cut.size(), 2u);

    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetOffset(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetOffset(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetSeqNo(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetSeqNo(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetLogicalMessageCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetLogicalMessageCount(), 1u);
    UNIT_ASSERT(!cut[0].HasUncompressedSize());
    UNIT_ASSERT(!cut[1].HasUncompressedSize());

    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetPartitionKey(), "k0");
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetPartitionKey(), "k1");
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetWriteTimestampMS(), 777);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetWriteTimestampMS(), 777);
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetCreateTimestampMS(), 1005);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetCreateTimestampMS(), 1007);

    const auto chunk0 = NKikimr::GetDeserializedData(cut[0].GetData());
    const auto chunk1 = NKikimr::GetDeserializedData(cut[1].GetData());
    UNIT_ASSERT_VALUES_EQUAL(chunk0.GetCodec(), expectedCodec);
    UNIT_ASSERT_VALUES_EQUAL(chunk1.GetCodec(), expectedCodec);
    UNIT_ASSERT_VALUES_EQUAL(chunk0.GetSeqNo(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(chunk1.GetSeqNo(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(DecompressPayload(chunk0.GetData(), expectedCodec), "value0");
    UNIT_ASSERT_VALUES_EQUAL(DecompressPayload(chunk1.GetData(), expectedCodec), "value1");
}

} // namespace

Y_UNIT_TEST_SUITE(TBatchCutterTest) {
    Y_UNIT_TEST(CutUncompressedKafkaBatchInDataChunk) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload());
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10));
    }

    Y_UNIT_TEST(CutGzipCompressedKafkaBatchInDataChunk) {
        const auto readResult = MakeKafkaBatchReadResult(
            MakeKafkaBatchPayload(NKafka::ECompressionType::GZIP));
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10), NPersQueueCommon::GZIP);
    }

    Y_UNIT_TEST(CutFailsOnNonRawDataChunkCodec) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload(), NPersQueueCommon::GZIP);
        UNIT_ASSERT_EXCEPTION(
            TKafkaBatchCutter().Cut(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10),
            yexception);
    }

    Y_UNIT_TEST(CutIgnoresOuterUncompressedSize) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload(), KafkaBatchCodec(), 123);
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10));
    }

    Y_UNIT_TEST(CutSkipsRecordsBeforeReadStartOffset) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload());
        const auto cut = TKafkaBatchCutter().Cut(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 11);
        UNIT_ASSERT_VALUES_EQUAL(cut.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cut[0].GetOffset(), 11u);
    }

    Y_UNIT_TEST(GetKeysReturnsKafkaRecordKeysWithLogicalOffsets) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload());
        const auto keys = TKafkaBatchCutter().GetKeys(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10);

        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("k0"), 10u);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("k1"), 11u);
    }

    Y_UNIT_TEST(GetKeysSkipsRecordsBeforeReadStartOffset) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload());
        const auto keys = TKafkaBatchCutter().GetKeys(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 11);

        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1u);
        UNIT_ASSERT(!keys.contains("k0"));
        UNIT_ASSERT_VALUES_EQUAL(keys.at("k1"), 11u);
    }

    Y_UNIT_TEST(GetKeysSkipsRecordsWithoutKey) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayloadWithNullKey());
        const auto keys = TKafkaBatchCutter().GetKeys(TBatchCutterData(readResult, NKikimr::GetDeserializedData(readResult.GetData())), 10);

        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("k1"), 11u);
    }

    Y_UNIT_TEST(NonRegularChunkIsNotCut) {
        NKikimrPQClient::TDataChunk chunk;
        chunk.SetChunkType(NKikimrPQClient::TDataChunk::GROW);
        chunk.SetCodec(KafkaBatchCodec());
        chunk.SetData(MakeKafkaBatchPayload());

        TReadResult readResult;
        NKikimrPQClient::TDataChunk dataChunk = chunk;
        readResult.SetData(SerializeDataChunk(std::move(chunk)));

        const auto cut = TKafkaBatchCutter().Cut(TBatchCutterData(readResult, std::move(dataChunk)), 10);
        UNIT_ASSERT_VALUES_EQUAL(cut.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cut[0].GetData(), readResult.GetData());
    }
}

} // namespace NKikimr::NPQ::NBatching
