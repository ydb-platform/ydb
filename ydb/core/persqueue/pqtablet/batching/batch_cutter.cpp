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

NPersQueueCommon::ECodec ToDataChunkCodec(NKafka::ECompressionType compressionType) {
    switch (compressionType) {
        case NKafka::ECompressionType::GZIP:
            return NPersQueueCommon::GZIP;
        case NKafka::ECompressionType::ZSTD:
            return NPersQueueCommon::ZSTD;
        default:
            return NPersQueueCommon::RAW;
    }
}

TString CompressPayload(TStringBuf data, NPersQueueCommon::ECodec codec) {
    if (codec == NPersQueueCommon::RAW) {
        return TString(data);
    }

    TString result;
    TStringOutput output(result);
    switch (codec) {
        case NPersQueueCommon::GZIP: {
            TZLibCompress gzip(&output, ZLib::GZip);
            gzip.Write(data.data(), data.size());
            gzip.Finish();
            output.Finish();
            return result;
        }
        case NPersQueueCommon::ZSTD: {
            TZstdCompress zstd(&output);
            zstd.Write(data.data(), data.size());
            zstd.Finish();
            output.Finish();
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

    Y_ENSURE(!dataChunk.HasCodec() || dataChunk.GetCodec() == NPersQueueCommon::RAW);

    const auto batch = NKafka::ReadKafkaRecordBatch(dataChunk.GetData());
    if (batch.Records.empty()) {
        return {readResult};
    }

    const auto codec = ToDataChunkCodec(batch.CompressionType());

    TVector<TReadResult> result;
    result.reserve(batch.Records.size());

    const ui64 baseOffset = readResult.GetOffset();
    for (size_t i = 0; i < batch.Records.size(); ++i) {
        const auto offset = baseOffset + batch.Records[i].OffsetDelta;
        if (offset < readStartOffset) {
            continue;
        }

        const auto& record = batch.Records[i];
        const ui64 seqNo = NKafka::GetRecordSeqNo(batch, i, record);
        TReadResult item = readResult;
        item.SetOffset(offset);
        item.SetSeqNo(seqNo);
        item.SetMessageCount(1);
        item.SetMessageFormat(NKikimrClient::STANDARD);
        item.ClearUncompressedSize();

        NKikimrPQClient::TDataChunk itemChunk = dataChunk;
        itemChunk.SetSeqNo(seqNo);
        itemChunk.SetCodec(codec);
        if (record.Value) {
            itemChunk.SetData(CompressPayload(TStringBuf(record.Value->data(), record.Value->size()), codec));
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
            item.SetCreateTimestampMS(timestamp);
        }
        result.push_back(std::move(item));
    }

    return result;
}

} // namespace NKikimr::NPQ::NBatching
