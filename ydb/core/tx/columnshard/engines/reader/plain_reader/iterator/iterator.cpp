#include "iterator.h"

namespace NKikimr::NOlap::NReader::NPlain {

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData->ExtractReadyResults(MaxRowsInBatch);
    i64 limitLeft = Context->GetReadMetadata()->Limit == 0 ? INT64_MAX : Context->GetReadMetadata()->Limit - ItemsRead;
    for (size_t i = 0; i < ready.size() && limitLeft; ++i) {
        auto& batch = ReadyResults.emplace_back(std::move(ready[i]));
        if (batch->GetResultBatch().num_rows() > limitLeft) {
            batch->Cut(limitLeft);
        }
        limitLeft -= batch->GetResultBatch().num_rows();
        ItemsRead += batch->GetResultBatch().num_rows();
    }

    if (limitLeft == 0) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "limit_reached_on_scan")("limit", Context->GetReadMetadata()->Limit)(
            "ready", ItemsRead);
        IndexedData->Abort("records count limit exhausted");
    }
}

}   // namespace NKikimr::NOlap::NReader::NPlain
