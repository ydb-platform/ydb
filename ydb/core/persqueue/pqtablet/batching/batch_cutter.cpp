#include "batch_cutter.h"

#include <ydb/library/kafka/kafka_records.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

namespace NKikimr::NPQ::NBatching {

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
        itemChunk.SetCodec(NPersQueueCommon::RAW);
        if (record.Value) {
            itemChunk.SetData(TString(record.Value->data(), record.Value->size()));
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
