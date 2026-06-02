#include "batch_cutter.h"

#include <ydb/core/persqueue/public/kafka_batch/kafka_batch.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/yexception.h>
#include <util/stream/mem.h>
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

TString GetKafkaBatchPayload(const TReadResult& readResult, const NKikimrPQClient::TDataChunk& dataChunk) {
    const auto codec = GetDataChunkCodec(dataChunk);
    TStringBuf payload = dataChunk.GetData();
    if (readResult.GetUncompressedSize() == 0) {
        return TString(payload);
    }
    return DecompressPayload(payload, codec);
}

} // namespace

TVector<TReadResult> TKafkaBatchCutter::Cut(const TReadResult& readResult) const {
    const NKikimrPQClient::TDataChunk dataChunk = NKikimr::GetDeserializedData(readResult.GetData());
    if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
        return {readResult};
    }

    const auto kafkaBatchPayload = GetKafkaBatchPayload(readResult, dataChunk);
    const auto records = NKafkaBatch::ParseKafkaBatchRecords(kafkaBatchPayload);
    if (records.empty()) {
        return {readResult};
    }

    TVector<TReadResult> result;
    result.reserve(records.size());

    const ui64 baseOffset = readResult.GetOffset();
    for (size_t i = 0; i < records.size(); ++i) {
        const auto& record = records[i];
        TReadResult item = readResult;
        item.SetOffset(baseOffset + i);
        item.SetMessageCount(1);
        item.SetMessageFormat(NKikimrClient::STANDARD);
        item.ClearUncompressedSize();

        NKikimrPQClient::TDataChunk itemChunk = dataChunk;
        itemChunk.SetCodec(NPersQueueCommon::RAW);
        if (record.Value) {
            itemChunk.SetData(*record.Value);
        } else {
            itemChunk.ClearData();
        }
        TString serializedChunk;
        Y_ENSURE(itemChunk.SerializeToString(&serializedChunk));
        item.SetData(std::move(serializedChunk));

        if (record.Key) {
            item.SetPartitionKey(*record.Key);
        }
        if (record.Timestamp > 0) {
            item.SetWriteTimestampMS(record.Timestamp);
            item.SetCreateTimestampMS(record.Timestamp);
        }
        result.push_back(std::move(item));
    }

    return result;
}

} // namespace NKikimr::NPQ::NBatching
