#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/duplicates/merge.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader {

void TRangeIndex::AddRange(const ui32 l, const ui32 r, const ui64 id) {
    AFL_VERIFY(Intervals.emplace(id, std::pair<ui32, ui32>({l, r})).second);
}

void TRangeIndex::RemoveRange(const ui64 id) {
    AFL_VERIFY(Intervals.erase(id));
}

std::vector<ui64> TRangeIndex::FindIntersections(const ui64 p) const {
    std::vector<ui64> result;
    for (const auto& [id, interval] : Intervals) {
        if (interval.first <= p && interval.second >= p) {
            result.emplace_back(id);
        }
    }
    return result;
}

void TIntervalCounter::Inc(const ui32 l, const ui32 r) {
    for (ui32 i = l; i <= r; ++i) {
        ++Count[i];
    }
}

TIntervalCounter::TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals) {
    for (const auto& [l, r] : intervals) {
        Inc(l, r);
    }
}

bool TIntervalCounter::IsAllZeros() const {
    return Count.empty();
}

std::vector<ui32> TIntervalCounter::DecAndGetZeros(const ui32 l, const ui32 r) {
    std::vector<ui32> zeros;
    for (ui32 i = l; i <= r; ++i) {
        auto find = Count.find(i);
        AFL_VERIFY(find != Count.end());
        --find->second;
        if (!find->second) {
            Count.erase(find);
            zeros.emplace_back(i);
        }
    }
    return zeros;
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter) {
    const ui32 localIdx = intervalIdx - FirstIntervalIdx;
    AFL_VERIFY(localIdx < IntervalFilters.size())("idx", localIdx)("size", IntervalFilters.size());
    AFL_VERIFY(!IntervalFilters[localIdx])("interval", intervalIdx);
    IntervalFilters[localIdx].emplace(std::move(filter));
    ++ReadyFilterCount;
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::Finish() && {
    AFL_VERIFY(IsReady());
    NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildAllowFilter();
    for (ui64 i = 0; i < IntervalFilters.size(); ++i) {
        result.Append(*TValidator::CheckNotNull(IntervalFilters[i]));
    }
    AFL_VERIFY(result.GetRecordsCountVerified() == Source->GetRecordsCount())("filter", result.GetRecordsCountVerified())(
                                                       "source", Source->GetRecordsCount());
    Subscriber->OnFilterReady(result);
}

TDuplicateFilterConstructor::TSourceIntervals::TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
    class TBorderView {
    private:
        using TReplaceKeyView = const NArrow::TReplaceKey*;
        YDB_READONLY_DEF(bool, IsLast);
        YDB_READONLY_DEF(TReplaceKeyView, PK);
        YDB_READONLY_DEF(ui64, PortionId);

        TBorderView(const bool isLast, const NArrow::TReplaceKey* const pk, const ui64 portionId)
            : IsLast(isLast)
            , PK(pk)
            , PortionId(portionId) {
        }

    public:
        static TBorderView First(const std::shared_ptr<TPortionInfo>& portion) {
            return TBorderView(false, &portion->IndexKeyStart(), portion->GetPortionId());
        }
        static TBorderView Last(const std::shared_ptr<TPortionInfo>& portion) {
            return TBorderView(true, &portion->IndexKeyEnd(), portion->GetPortionId());
        }

        std::partial_ordering operator<=>(const TBorderView& other) const {
            return std::tie<const NArrow::TReplaceKey&, const bool&>(*PK, !IsLast) <=>
                   std::tie<const NArrow::TReplaceKey&, const bool&>(*other.PK, !other.IsLast);
        };
    };

    std::vector<TBorderView> borders;
    for (const auto& portion : portions) {
        borders.emplace_back(TBorderView::First(portion));
        borders.emplace_back(TBorderView::Last(portion));
    }
    std::sort(borders.begin(), borders.end());

    THashMap<ui64, ui32> firstByPortionId;
    for (const auto& border : borders) {
        if (border.GetIsLast()) {
            if (IntervalBorders.empty() || IntervalBorders.back() != *border.GetPK()) {
                IntervalBorders.emplace_back(*border.GetPK());
            }
            const TIntervalsRange sourceRange(
                *TValidator::CheckNotNull(firstByPortionId.FindPtr(border.GetPortionId())), IntervalBorders.size() - 1);
            AFL_VERIFY(SourceRanges.emplace(border.GetPortionId(), sourceRange).second);
        } else {
            AFL_VERIFY(firstByPortionId.emplace(border.GetPortionId(), IntervalBorders.size()).second);
        }
    }
    AFL_VERIFY(SourceRanges.size() == portions.size());
}

void TDuplicateFilterConstructor::Handle(const TEvRequestFilter::TPtr& ev) {
    const auto& source = ev->Get()->GetSource();
    Y_UNUSED(Intervals.GetRangeVerified(source->GetSourceId()));
    AFL_VERIFY(AvailableSources.emplace(source->GetSourceId(), TSourceFilterConstructor(source, ev->Get()->GetSubscriber(), Intervals)).second);

    TIntervalsRange range = Intervals.GetRangeVerified(source->GetSourceIdx());
    AvailableSourcesCount.AddRange(range.GetFirstIdx(), range.GetLastIdx(), source->GetSourceIdx());
    std::vector<ui32> readyIntervals = AwaitedSourcesCount.DecAndGetZeros(range.GetFirstIdx(), range.GetLastIdx());

    for (const ui32 intervalIdx : readyIntervals) {
        std::vector<std::shared_ptr<NCommon::IDataSource>> sources;
        for (const ui64 portionId : AvailableSourcesCount.FindIntersections(intervalIdx)) {
            sources.emplace_back(TValidator::CheckNotNull(AvailableSources.FindPtr(portionId))->GetSource());
        }
        AFL_VERIFY(sources.size());
        const std::shared_ptr<NCommon::TSpecialReadContext> readContext = sources.front()->GetContext();
        const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
            Intervals.GetLeftExlusiveBorder(intervalIdx), Intervals.GetRightInclusiveBorder(intervalIdx),
            readContext->GetReadMetadata()->GetReplaceKey(), IIndexInfo::GetSnapshotColumnNames());
        for (const auto& source : sources) {
            // TODO: why table has applied filter and not applied filter?
            // TODO: make slice
            task->AddSource({} /*TODO*/, source->GetStageData().GetAppliedFilter(),
                std::make_shared<TInternalFilterSubscriber>(intervalIdx, source->GetSourceId(), SelfId()));
        }
        NConveyor::TScanServiceOperator::SendTaskToExecute(task, readContext->GetCommonContext()->GetConveyorProcessId());
    }

    if (AwaitedSourcesCount.IsAllZeros()) {
        PassAway();
    }
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterPartialResult::TPtr& ev) {
    if (ev->Get()->GetResult().IsFail()) {
        // TODO: abort
        return;
    }
    TSourceFilterConstructor* constructor = AvailableSources.FindPtr(ev->Get()->GetPortionId());
    AFL_VERIFY(constructor)("portion", ev->Get()->GetPortionId());
    // TODO: avoid copying filters
    constructor->SetFilter(ev->Get()->GetIntervalIdx(), ev->Get()->ExtractResult().DetachResult());
    if (constructor->IsReady()) {
        std::move(*constructor).Finish();
        AFL_VERIFY(AvailableSources.erase(ev->Get()->GetPortionId()));
        // TODO: if done, pass away
    }
}

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& portions)
    : Intervals(portions)
    , AwaitedSourcesCount([this]() {
        std::vector<std::pair<ui32, ui32>> intervals;
        for (const auto& [_, interval] : Intervals.GetSourceRanges()) {
            intervals.emplace_back(interval);
        }
        return intervals;
    }()) {
}

}   // namespace NKikimr::NOlap::NReader
