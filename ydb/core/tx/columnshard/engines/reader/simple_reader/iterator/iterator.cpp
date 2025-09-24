#include "iterator.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData->ExtractReadyResults(MaxRowsInBatch);
    const i64 limitLeft = Context->GetReadMetadata()->GetLimitController().GetLimitRobust();
    for (size_t i = 0; i < ready.size(); ++i) {
        auto& batch = ReadyResults.emplace_back(std::move(ready[i]));
        AFL_VERIFY(batch->GetResultBatch().num_rows() <= limitLeft)("count", batch->GetResultBatch().num_rows())("limit", limitLeft);
        ItemsRead += batch->GetResultBatch().num_rows();
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
