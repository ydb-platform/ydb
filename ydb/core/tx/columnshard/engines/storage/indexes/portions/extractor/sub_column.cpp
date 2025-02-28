#include "sub_column.h"
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

namespace NKikimr::NOlap::NIndexes {

void TSubColumnDataExtractor::DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
    const TRecordVisitor& recordVisitor) const {
    AFL_VERIFY(dataArray->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray);
    const auto subColumns = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsArray>(dataArray);
    if (auto idxColumn = subColumns->GetColumnsData().GetStats().GetKeyIndexOptional(SubColumnName)) {
        auto chunkedArray = subColumns->GetColumnsData().GetRecords()->GetColumnVerified(*idxColumn)->GetChunkedArray();
        for (auto&& i : chunkedArray->chunks()) {
            chunkVisitor(i, 0);
        }
    } else if (auto idxColumn = subColumns->GetOthersData().GetStats().GetKeyIndexOptional(SubColumnName)) {
        auto iterator = subColumns->GetOthersData().BuildIterator();
        for (; iterator.IsValid(); iterator.Next()) {
            if (iterator.GetKeyIndex() != *idxColumn) {
                continue;
            }
            recordVisitor(iterator.GetValue(), 0);
        }
    }
}

}   // namespace NKikimr::NOlap::NIndexes
