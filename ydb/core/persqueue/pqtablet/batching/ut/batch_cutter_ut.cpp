#include <ydb/core/persqueue/pqtablet/batching/batch_cutter.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/stream/mem.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>

namespace NKikimr::NPQ::NBatching {
namespace {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;

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
    WriteInt8(body, 0);
    WriteVarInt<i64>(body, timestampDelta);
    WriteVarInt<i64>(body, offsetDelta);
    WriteVarBytes(body, key);
    WriteVarBytes(body, value);
    WriteVarInt<i32>(body, 0);

    WriteVarInt<i32>(out, body.size());
    out += body;
}

TString MakeKafkaBatchPayload() {
    TString records;
    WriteInt32(records, 2);
    WriteRecord(records, 5, 0, "k0", "value0");
    WriteRecord(records, 7, 1, "k1", "value1");

    TString body;
    WriteInt32(body, -1);
    WriteInt8(body, 2);
    WriteInt32(body, 0);
    WriteInt16(body, 0);
    WriteInt32(body, 1);
    WriteInt64(body, 1000);
    WriteInt64(body, 1007);
    WriteInt64(body, 42);
    WriteInt16(body, 3);
    WriteInt32(body, 10);
    body += records;

    TString batch;
    WriteInt64(batch, 100);
    WriteInt32(batch, body.size());
    batch += body;
    return batch;
}

TString CompressGzip(TStringBuf data) {
    TString compressed;
    TStringOutput output(compressed);
    TZLibCompress gzip(&output, ZLib::GZip);
    gzip.Write(data.data(), data.size());
    gzip.Finish();
    return compressed;
}

TString CompressZstd(TStringBuf data) {
    TString compressed;
    TStringOutput output(compressed);
    TZstdCompress zstd(&output);
    zstd.Write(data.data(), data.size());
    zstd.Finish();
    return compressed;
}

TString SerializeDataChunk(NKikimrPQClient::TDataChunk chunk) {
    TString serialized;
    Y_ENSURE(chunk.SerializeToString(&serialized));
    return serialized;
}

TReadResult MakeKafkaBatchReadResult(
    TString payload,
    NPersQueueCommon::ECodec codec,
    ui64 uncompressedSize,
    NKikimrPQClient::TDataChunk::EChunkType chunkType = NKikimrPQClient::TDataChunk::REGULAR)
{
    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(chunkType);
    chunk.SetCodec(codec);
    chunk.SetData(std::move(payload));

    TReadResult readResult;
    readResult.SetOffset(10);
    readResult.SetMessageCount(2);
    readResult.SetMessageFormat(NKikimrClient::KAFKA_BATCH);
    readResult.SetData(SerializeDataChunk(std::move(chunk)));
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

void AssertKafkaBatchCut(const TVector<TReadResult>& cut, NPersQueueCommon::ECodec expectedCodec) {
    UNIT_ASSERT_VALUES_EQUAL(cut.size(), 2u);

    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetOffset(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetOffset(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetMessageCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetMessageCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(cut[0].GetMessageFormat()), static_cast<ui32>(NKikimrClient::STANDARD));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(cut[1].GetMessageFormat()), static_cast<ui32>(NKikimrClient::STANDARD));
    UNIT_ASSERT(!cut[0].HasUncompressedSize());
    UNIT_ASSERT(!cut[1].HasUncompressedSize());

    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetPartitionKey(), "k0");
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetPartitionKey(), "k1");
    UNIT_ASSERT_VALUES_EQUAL(cut[0].GetWriteTimestampMS(), 1005);
    UNIT_ASSERT_VALUES_EQUAL(cut[1].GetWriteTimestampMS(), 1007);

    const auto chunk0 = NKikimr::GetDeserializedData(cut[0].GetData());
    const auto chunk1 = NKikimr::GetDeserializedData(cut[1].GetData());
    UNIT_ASSERT_VALUES_EQUAL(chunk0.GetCodec(), expectedCodec);
    UNIT_ASSERT_VALUES_EQUAL(chunk1.GetCodec(), expectedCodec);
    UNIT_ASSERT_VALUES_EQUAL(DecompressPayload(chunk0.GetData(), expectedCodec), "value0");
    UNIT_ASSERT_VALUES_EQUAL(DecompressPayload(chunk1.GetData(), expectedCodec), "value1");
}

} // namespace

Y_UNIT_TEST_SUITE(TBatchCutterTest) {
    Y_UNIT_TEST(CutUncompressedKafkaBatchInDataChunk) {
        const auto readResult = MakeKafkaBatchReadResult(MakeKafkaBatchPayload(), NPersQueueCommon::RAW, 0);
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(readResult), NPersQueueCommon::RAW);
    }

    Y_UNIT_TEST(CutGzipCompressedKafkaBatchInDataChunk) {
        const auto payload = MakeKafkaBatchPayload();
        const auto readResult = MakeKafkaBatchReadResult(
            CompressGzip(payload),
            NPersQueueCommon::GZIP,
            payload.size());
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(readResult), NPersQueueCommon::GZIP);
    }

    Y_UNIT_TEST(CutZstdCompressedKafkaBatchInDataChunk) {
        const auto payload = MakeKafkaBatchPayload();
        const auto readResult = MakeKafkaBatchReadResult(
            CompressZstd(payload),
            NPersQueueCommon::ZSTD,
            payload.size());
        AssertKafkaBatchCut(TKafkaBatchCutter().Cut(readResult), NPersQueueCommon::ZSTD);
    }

    Y_UNIT_TEST(NonRegularChunkIsNotCut) {
        NKikimrPQClient::TDataChunk chunk;
        chunk.SetChunkType(NKikimrPQClient::TDataChunk::GROW);
        chunk.SetCodec(NPersQueueCommon::RAW);
        chunk.SetData(MakeKafkaBatchPayload());

        TReadResult readResult;
        readResult.SetMessageFormat(NKikimrClient::KAFKA_BATCH);
        readResult.SetData(SerializeDataChunk(std::move(chunk)));

        const auto cut = TKafkaBatchCutter().Cut(readResult);
        UNIT_ASSERT_VALUES_EQUAL(cut.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cut[0].GetData(), readResult.GetData());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(cut[0].GetMessageFormat()), static_cast<ui32>(NKikimrClient::KAFKA_BATCH));
    }
}

} // namespace NKikimr::NPQ::NBatching
