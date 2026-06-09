#include "batch_cutter.h"

#include <ydb/library/kafka/kafka_records.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/yexception.h>
#include <util/stream/mem.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>

namespace NKikimr::NPQ::NBatching {
namespace {

NPersQueueCommon::ECodec GetDataChunkCodec(const NKikimrPQClient::TDataChunk& dataChunk) {
    if (!dataChunk.HasCodec()) {
        return NPersQueueCommon::RAW;
    }

    switch (static_cast<NPersQueueCommon::ECodec>(dataChunk.GetCodec())) {
        case NPersQueueCommon::RAW:
        case NPersQueueCommon::GZIP:
        case NPersQueueCommon::ZSTD:
            return static_cast<NPersQueueCommon::ECodec>(dataChunk.GetCodec());
        default:
            ythrow yexception() << "unsupported message codec: " << dataChunk.GetCodec();
    }
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

TString GetKafkaBatchPayload(const TReadResult& readResult, const NKikimrPQClient::TDataChunk& dataChunk, NPersQueueCommon::ECodec codec) {
    TStringBuf payload = dataChunk.GetData();
    if (readResult.GetUncompressedSize() == 0) {
        return TString(payload);
    }
    return DecompressPayload(payload, codec);
}

TString CompressPayload(TString payload, NPersQueueCommon::ECodec codec) {
    if (codec == NPersQueueCommon::RAW) {
        return payload;
    }

    TString result;
    switch (codec) {
        case NPersQueueCommon::GZIP: {
            TStringOutput output(result);
            TZLibCompress gzip(&output, ZLib::GZip);
            gzip << payload;
            gzip.Finish();
            return result;
        }
        case NPersQueueCommon::ZSTD: {
            TStringOutput output(result);
            TZstdCompress zstd(&output);
            zstd << payload;
            zstd.Finish();
            return result;
        }

        default:
            ythrow yexception() << "unsupported message codec: " << static_cast<int>(codec);
    }
}

} // namespace

TVector<TReadResult> TKafkaBatchCutter::Cut(const TReadResult& readResult, const ui64 readStartOffset) const {
    const NKikimrPQClient::TDataChunk dataChunk = NKikimr::GetDeserializedData(readResult.GetData());
    if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
        return {readResult};
    }

    const auto codec = GetDataChunkCodec(dataChunk);
    const auto kafkaBatchPayload = GetKafkaBatchPayload(readResult, dataChunk, codec);
    const auto batch = NKafka::ReadKafkaRecordBatch(kafkaBatchPayload);
    if (batch.Records.empty()) {
        return {readResult};
    }

    TVector<TReadResult> result;
    result.reserve(batch.Records.size());

    const ui64 baseOffset = readResult.GetOffset();
    for (size_t i = 0; i < batch.Records.size(); ++i) {
        const auto offset = baseOffset + batch.Records[i].OffsetDelta;
        if (offset < readStartOffset) {
            continue;
        }

        const auto& record = batch.Records[i];
        TReadResult item = readResult;
        item.SetOffset(offset);
        item.SetMessageCount(1);
        item.SetMessageFormat(NKikimrClient::STANDARD);
        item.ClearUncompressedSize();

        NKikimrPQClient::TDataChunk itemChunk = dataChunk;
        itemChunk.SetCodec(codec);
        if (record.Value) {
            itemChunk.SetData(CompressPayload(TString(record.Value->data(), record.Value->size()), codec));
        } else {
            itemChunk.ClearData();
        }
        TString serializedChunk;
        Y_ENSURE(itemChunk.SerializeToString(&serializedChunk));
        item.SetData(std::move(serializedChunk));

        if (record.Key) {
            item.SetPartitionKey(TString(record.Key->data(), record.Key->size()));
        }
        const i64 timestamp = batch.BaseTimestamp + record.TimestampDelta;
        if (timestamp > 0) {
            item.SetWriteTimestampMS(timestamp);
            item.SetCreateTimestampMS(timestamp);
        }
        result.push_back(std::move(item));
    }

    return result;
}

} // namespace NKikimr::NPQ::NBatching
