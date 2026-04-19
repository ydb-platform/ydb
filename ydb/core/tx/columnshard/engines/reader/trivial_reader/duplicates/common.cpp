#include "common.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

TPortionStore::TPortionStore(THashMap<ui64, TPortionInfo::TConstPtr>&& portions)
    : Portions(std::move(portions))
{
}

TPortionInfo::TConstPtr TPortionStore::GetPortionVerified(const ui64 portionId) const {
    auto* findPortion = Portions.FindPtr(portionId);
    AFL_VERIFY(findPortion)("portion", portionId);
    return *findPortion;
}

TBorder::TBorder(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const std::vector<ui64>& portionIds)
    : Key(key)
    , PortionIds(portionIds)
{
}

TString TBorder::DebugString() const {
    return TStringBuilder() << "{" << "PortionIds=" << JoinSeq(",", PortionIds) << ";Key=" << Key->GetSorting()->DebugJson(0) << "}";
}

void TBordersBatch::AddBorder(const TBorder& border) {
    Borders.push_back(border);
    PortionIds.insert(border.GetPortionIds().begin(), border.GetPortionIds().end());
}

TBordersIterator::TBordersIterator(
    std::vector<TBorder>&& borders, const ui64 portionsCountSoftLimit)
    : Borders(std::move(borders))
    , PortionsCountSoftLimit(portionsCountSoftLimit)
{
}

TBordersBatch TBordersIterator::Next() {
    AFL_VERIFY(NextBorder < Borders.size());
    TBordersBatch batch;
    for (; NextBorder < Borders.size() && (batch.GetPortionIds().size() < PortionsCountSoftLimit || Borders[NextBorder].GetPortionIds().empty()); ++NextBorder) {
        batch.AddBorder(Borders[NextBorder]);
    }
    return batch;
}

bool TBordersIterator::IsDone() const {
    return NextBorder == Borders.size();
}

void TBordersIteratorBuilder::AppendBorder(const TBorder& border) {
    Borders.push_back(border);
}

TBordersIterator TBordersIteratorBuilder::Build() {
    return TBordersIterator(std::move(Borders), BATCH_PORTIONS_COUNT_SOFT_LIMIT);
}

ui64 TBordersIteratorBuilder::NumBorders() const {
    return Borders.size();
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
