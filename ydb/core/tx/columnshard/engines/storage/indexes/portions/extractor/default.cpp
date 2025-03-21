#include "default.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

#include <util/digest/fnv.h>

namespace NKikimr::NOlap::NIndexes {

void TDefaultDataExtractor::VisitSimple(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const ui64 hashBase, const TChunkVisitor& visitor) const {
    const auto visitorLocal = [&](const std::shared_ptr<arrow::Array>& arr) {
        visitor(arr, hashBase);
    };
    dataArray->VisitValues(visitorLocal);
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
        const ui64 hashBase = NRequest::TOriginalDataAddress::CalcSubColumnHash(svColName);
        VisitSimple(subColumns->GetColumnsData().GetRecords()->GetColumnVerified(idx), hashBase, chunkVisitor);
    }
    std::vector<ui64> hashByColumnIdx;
    for (ui32 idx = 0; idx < subColumns->GetOthersData().GetStats().GetColumnsCount(); ++idx) {
        const std::string_view svColName = subColumns->GetOthersData().GetStats().GetColumnName(idx);
        hashByColumnIdx.emplace_back(NRequest::TOriginalDataAddress::CalcSubColumnHash(svColName));
    }
    auto iterator = subColumns->GetOthersData().BuildIterator();
    for (; iterator.IsValid(); iterator.Next()) {
        recordVisitor(iterator.GetValue(), hashByColumnIdx[iterator.GetKeyIndex()]);
    }
}

bool TDefaultDataExtractor::DoCheckForIndex(const NRequest::TOriginalDataAddress& request, ui64& hashBase) const {
    if (request.GetSubColumnName()) {
        std::string_view sv = [&]() {
            if (request.GetSubColumnName().StartsWith("$.")) {
                return std::string_view(request.GetSubColumnName().data() + 2, request.GetSubColumnName().size() - 2);
            } else {
                return std::string_view(request.GetSubColumnName().data(), request.GetSubColumnName().size());
            }
        }();
        if (sv.starts_with("\"") && sv.ends_with("\"")) {
            sv = std::string_view(sv.data() + 1, sv.size() - 2);
        }
        hashBase = NRequest::TOriginalDataAddress::CalcSubColumnHash(sv);
    }
    return true;
}

THashMap<ui64, ui32> TDefaultDataExtractor::DoGetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const {
    THashMap<ui64, ui32> result;
    if (dataArray->GetType() != NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
        result.emplace(0, dataArray->GetRecordsCount());
    } else {
        const auto subColumns = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsArray>(dataArray);
        {
            auto& stats = subColumns->GetColumnsData().GetStats();
            for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                result[NRequest::TOriginalDataAddress::CalcSubColumnHash(stats.GetColumnName(i))] += stats.GetColumnRecordsCount(i);
            }
        }
        {
            auto& stats = subColumns->GetOthersData().GetStats();
            for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                result[NRequest::TOriginalDataAddress::CalcSubColumnHash(stats.GetColumnName(i))] += stats.GetColumnRecordsCount(i);
            }
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes
