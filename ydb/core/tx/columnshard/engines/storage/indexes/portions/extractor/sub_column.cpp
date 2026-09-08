#include "sub_column.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

namespace NKikimr::NOlap::NIndexes {

namespace {

std::optional<NArrow::NAccessor::NSubColumns::TResolvedPathMatch> ResolveIndexPath(
    const NArrow::NAccessor::TSubColumnsArray& subColumns, const TStringBuf keyName) {
    auto result = NArrow::NAccessor::NSubColumns::ResolveBestPath(
        subColumns.GetColumnsData().GetStats(), subColumns.GetOthersData().GetStats(), NArrow::NAccessor::NSubColumns::ToJsonPath(keyName));
    AFL_VERIFY(result.IsSuccess())("key", keyName)("error", result.GetErrorMessage());
    auto path = result.DetachResult();
    if (!path || path->Path.RemainingPath) {
        return std::nullopt;
    }
    return path;
}

}   // namespace

void TSubColumnDataExtractor::DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray,
    const TChunkVisitor& /*chunkVisitor*/, const TRecordVisitor& recordVisitor) const {
    AFL_VERIFY(dataArray->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray);
    const auto subColumns = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsArray>(dataArray);
    if (const auto path = ResolveIndexPath(*subColumns, SubColumnName); path && path->IsColumn) {
        auto iterator = subColumns->GetColumnsData().BuildIterator(path->Path.ColumnIndex);
        for (; iterator.IsValid(); iterator.Next()) {
            recordVisitor(iterator.GetValue(), 0);
        }
    } else if (const auto path = ResolveIndexPath(*subColumns, SubColumnName)) {
        auto iterator = subColumns->GetOthersData().BuildIterator();
        for (; iterator.IsValid(); iterator.Next()) {
            if (iterator.GetKeyIndex() != path->Path.ColumnIndex) {
                continue;
            }
            recordVisitor(iterator.GetValue(), 0);
        }
    }
}

THashMap<ui64, ui32> TSubColumnDataExtractor::DoGetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const {
    AFL_VERIFY(dataArray->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray);
    const auto subColumns = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsArray>(dataArray);
    THashMap<ui64, ui32> result;
    if (const auto path = ResolveIndexPath(*subColumns, SubColumnName); path && path->IsColumn) {
        result.emplace(NRequest::TOriginalDataAddress::CalcSubColumnHash(SubColumnName),
            subColumns->GetColumnsData().GetStats().GetColumnRecordsCount(path->Path.ColumnIndex));
    } else if (const auto path = ResolveIndexPath(*subColumns, SubColumnName)) {
        result.emplace(NRequest::TOriginalDataAddress::CalcSubColumnHash(SubColumnName),
            subColumns->GetOthersData().GetStats().GetColumnRecordsCount(path->Path.ColumnIndex));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes
