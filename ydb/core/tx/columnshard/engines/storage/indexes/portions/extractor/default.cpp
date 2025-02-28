#include "default.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

#include <util/digest/fnv.h>

namespace NKikimr::NOlap::NIndexes {

void TDefaultDataExtractor::VisitSimple(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const ui64 hashBase, const TChunkVisitor& visitor) const {
    auto chunkedArray = dataArray->GetChunkedArray();
    for (auto&& i : chunkedArray->chunks()) {
        visitor(i, hashBase);
    }
}

void TDefaultDataExtractor::DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
    const TRecordVisitor& recordVisitor) const {
    if (dataArray->GetType() != NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
        VisitSimple(dataArray, 0, chunkVisitor);
        return;
    }
    const auto subColumns = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsArray>(dataArray);
    for (ui32 idx = 0; idx < subColumns->GetColumnsData().GetRecords()->GetColumnsCount(); ++idx) {
        const std::string_view svColName = subColumns->GetColumnsData().GetStats().GetColumnName(idx);
        const ui64 hashBase = FnvHash<ui64>(svColName.data(), svColName.size());
        VisitSimple(subColumns->GetColumnsData().GetRecords()->GetColumnVerified(idx), hashBase, chunkVisitor);
    }
    std::vector<ui64> hashByColumnIdx;
    for (ui32 idx = 0; idx < subColumns->GetOthersData().GetStats().GetColumnsCount(); ++idx) {
        const std::string_view svColName = subColumns->GetOthersData().GetStats().GetColumnName(idx);
        hashByColumnIdx.emplace_back(FnvHash<ui64>(svColName.data(), svColName.size()));
    }
    auto iterator = subColumns->GetOthersData().BuildIterator();
    for (; iterator.IsValid(); iterator.Next()) {
        recordVisitor(iterator.GetValue(), hashByColumnIdx[iterator.GetKeyIndex()]);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
