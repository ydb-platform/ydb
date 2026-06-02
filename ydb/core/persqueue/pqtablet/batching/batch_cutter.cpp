#include "batch_cutter.h"

#include <ydb/core/persqueue/public/kafka_batch/kafka_batch.h>

namespace NKikimr::NPQ::NBatching {

TVector<TReadResult> TKafkaBatchCutter::Cut(const TReadResult& readResult) const {
    const auto records = NKafkaBatch::ParseKafkaBatchRecords(readResult.GetData());
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
        if (record.Value) {
            item.SetData(*record.Value);
        } else {
            item.ClearData();
        }
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
