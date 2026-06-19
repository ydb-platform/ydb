#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <ydb/library/kafka/kafka_messages_int.h>
#include <ydb/library/kafka/kafka_records.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/buffer.h>
#include <util/stream/zlib.h>

namespace NYdb::inline Dev::NTopic {

namespace {

class TZLibToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZLibCompress {
public:
    TZLibToStringCompressor(TBuffer& dst, ZLib::StreamType type, size_t quality)
        : TEmbedPolicy<TBufferOutput>(dst)
        , TZLibCompress(TEmbedPolicy::Ptr(), type, quality)
    {
    }
};

class TZstdToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZstdCompress {
public:
    TZstdToStringCompressor(TBuffer& dst, int quality)
        : TEmbedPolicy<TBufferOutput>(dst)
        , TZstdCompress(TEmbedPolicy::Ptr(), quality)
    {
    }
};

}

namespace {

std::string DecompressGzipData(const std::string& data) {
    TMemoryInput input(data.data(), data.size());
    TString result;
    TStringOutput resultOutput(result);
    TZLibDecompress inputStreamStorage(&input);
    TransferData(&inputStreamStorage, &resultOutput);
    return result;
}

std::string DecompressZstdData(const std::string& data) {
    TMemoryInput input(data.data(), data.size());
    TString result;
    TStringOutput resultOutput(result);
    TZstdDecompress inputStreamStorage(&input);
    TransferData(&inputStreamStorage, &resultOutput);
    return result;
}

std::string KafkaBytesToString(const NKafka::TKafkaBytes& bytes) {
    if (!bytes) {
        return {};
    }
    return std::string(bytes->data(), bytes->size());
}

TDecompressionResult MakeSingleMessageResult(std::string data) {
    TDecompressionResult result;
    result.Messages.push_back(TDecompressedMessage{
        .Data = std::move(data),
        .Meta = std::nullopt,
    });
    return result;
}

} // namespace

std::string TGzipCodec::Decompress(const std::string& data) const {
    return DecompressGzipData(data);
}

std::unique_ptr<IOutputStream> TGzipCodec::CreateCoder(TBuffer& result, int quality) const {
    return std::make_unique<TZLibToStringCompressor>(result, ZLib::GZip, quality >= 0 ? quality : 6);
}

std::string TZstdCodec::Decompress(const std::string& data) const {
    return DecompressZstdData(data);
}

std::unique_ptr<IOutputStream> TZstdCodec::CreateCoder(TBuffer& result, int quality) const {
    return std::make_unique<TZstdToStringCompressor>(result, quality);
}

std::string TUnsupportedCodec::Decompress(const std::string&) const {
    throw yexception() << "use of unsupported codec";
}

std::unique_ptr<IOutputStream> TUnsupportedCodec::CreateCoder(TBuffer&, int) const {
    throw yexception() << "use of unsupported codec";
}

TDecompressionResult ICodec::DecompressData(const std::string& data) const {
    return MakeSingleMessageResult(Decompress(data));
}

void ICodec::CompressWriteBlock(TWriteBlockCompression& ctx) const {
    TBuffer compressedData;
    std::unique_ptr<IOutputStream> coder = CreateCoder(compressedData, ctx.CompressionLevel);
    for (auto& buffer : ctx.Payloads) {
        coder->Write(buffer.data(), buffer.size());
    }
    coder->Finish();
    Y_ABORT_UNLESS(!compressedData.Empty());
    ctx.Data = std::move(compressedData);
    ctx.Compressed = true;
    ctx.CodecID = static_cast<ui32>(ctx.Codec);
}

std::string TKafkaBatchCodec::Decompress(const std::string& data) const {
    return TakeFirstDecompressedMessage(DecompressData(data));
}

TDecompressionResult TKafkaBatchCodec::DecompressData(const std::string& data) const {
    using namespace NKafka;

    TDecompressionResult result;
    const TKafkaRecordBatch kafkaBatch = ReadKafkaRecordBatch(data);
    result.BatchBaseOffset = kafkaBatch.BaseOffset;
    result.BatchBaseSequence = 0;
    result.BatchBaseTimestampMs = kafkaBatch.BaseTimestamp;
    result.Messages.reserve(kafkaBatch.Records.size());

    for (size_t i = 0; i < kafkaBatch.Records.size(); ++i) {
        const auto& record = kafkaBatch.Records[i];
        TDecompressedMessageMeta meta{
            .OffsetDelta = static_cast<i32>(record.OffsetDelta),
            .SequenceDelta = static_cast<i64>(GetRecordSeqNo(kafkaBatch, i, record)),
            .TimestampDelta = record.TimestampDelta,
        };
        result.Messages.push_back(TDecompressedMessage{
            .Data = KafkaBytesToString(record.Value),
            .Meta = std::move(meta),
        });
    }

    return result;
}

std::unique_ptr<IOutputStream> TKafkaBatchCodec::CreateCoder(TBuffer&, int) const {
    throw yexception() << "use of unsupported codec";
}

namespace {

NKafka::ECompressionType ToKafkaBatchInnerCompression(std::optional<ECodec> batchInnerCodec) {
    if (!batchInnerCodec) {
        return NKafka::ECompressionType::NONE;
    }
    switch (*batchInnerCodec) {
        case ECodec::GZIP:
            return NKafka::ECompressionType::GZIP;
        case ECodec::ZSTD:
            return NKafka::ECompressionType::ZSTD;
        default:
            ythrow yexception() << "unsupported batch inner codec: " << static_cast<uint32_t>(*batchInnerCodec);
    }
}

} // namespace

void TKafkaBatchCodec::CompressWriteBlock(TWriteBlockCompression& ctx) const {
    if (ctx.Payloads.size() == 1) {
        return;
    }

    using namespace NKafka;

    Y_ABORT_UNLESS(ctx.Payloads.size() == ctx.CreatedAt.size());
    Y_ABORT_UNLESS(!ctx.Payloads.empty());

    TKafkaRecordBatch kafkaBatch;
    kafkaBatch.Magic = 2;
    kafkaBatch.ProducerId = 0;
    kafkaBatch.ProducerEpoch = 0;
    kafkaBatch.BaseSequence = static_cast<TKafkaRecordBatch::BaseSequenceMeta::Type>(ctx.BaseSequence);
    kafkaBatch.Attributes = static_cast<TKafkaRecordBatch::AttributesMeta::Type>(
        ToKafkaBatchInnerCompression(ctx.BatchInnerCodec));

    const i64 baseTimestamp = static_cast<i64>(ctx.CreatedAt.front().MilliSeconds());
    kafkaBatch.BaseTimestamp = baseTimestamp;
    kafkaBatch.MaxTimestamp = baseTimestamp;
    kafkaBatch.LastOffsetDelta = static_cast<TKafkaRecordBatch::LastOffsetDeltaMeta::Type>(ctx.Payloads.size() - 1);

    for (size_t i = 0; i < ctx.Payloads.size(); ++i) {
        TKafkaRecord record;
        record.OffsetDelta = static_cast<TKafkaRecord::OffsetDeltaMeta::Type>(i);
        record.TimestampDelta = ctx.CreatedAt[i].MilliSeconds() - baseTimestamp;
        kafkaBatch.MaxTimestamp = Max<i64>(kafkaBatch.MaxTimestamp, static_cast<i64>(ctx.CreatedAt[i].MilliSeconds()));
        record.Value = TKafkaRawBytes(ctx.Payloads[i].data(), ctx.Payloads[i].size());
        record.Length = record.Size(2) - NKafka::NPrivate::SizeOfVarint<TKafkaRecord::LengthMeta::Type>(0);
        kafkaBatch.Records.push_back(std::move(record));
    }

    kafkaBatch.BatchLength = kafkaBatch.Size(2)
        - sizeof(TKafkaRecordBatch::BaseOffsetMeta::Type)
        - sizeof(TKafkaRecordBatch::BatchLengthMeta::Type);

    const TString serialized = WriteKafkaRecordBatch(kafkaBatch);
    ctx.Data = TBuffer(serialized.data(), serialized.size());
    ctx.Payloads.assign(1, std::string_view(ctx.Data.data(), ctx.Data.size()));
    ctx.CodecID = static_cast<ui32>(Ydb::Topic::CODEC_KAFKA_BATCH);
    ctx.Compressed = true;
}

class TCommonCodecsProvider {
public:
    TCommonCodecsProvider() {
        TCodecMap::GetTheCodecMap().Set((uint32_t)ECodec::GZIP, std::make_unique<TGzipCodec>());
        TCodecMap::GetTheCodecMap().Set((uint32_t)ECodec::ZSTD, std::make_unique<TZstdCodec>());
        TCodecMap::GetTheCodecMap().Set((uint32_t)ECodec::KAFKA_BATCH, std::make_unique<TKafkaBatchCodec>());
    }
};

namespace {
TCommonCodecsProvider COMMON_CODECS_PROVIDER;
}

}; // namespace NYdb::NTopic
