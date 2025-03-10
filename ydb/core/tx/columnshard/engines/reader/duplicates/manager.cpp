#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/duplicates/merge.h>
#include <ydb/core/tx/conveyor/usage/service.h>

#include <bit>

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
    // ui32 maxValue = 0;
    // for (const auto& [l, r] : intervals) {
    //     AFL_VERIFY(l <= r);
    //     if (r > maxValue) {
    //         maxValue = r;
    //     }
    // }
    // const ui32 size = std::bit_ceil(maxValue);
    // Count.resize(size * 2 - 1);
    // PropagateDelta.resize(size * 2 - 1);

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
    AFL_VERIFY(result.GetRecordsCountVerified() == Source->GetStageData().GetTable()->GetRecordsCountVerified())(
                                                       "filter", result.GetRecordsCountVerified())("source", Source->GetRecordsCount());
    Subscriber->OnFilterReady(result);
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::AbortConstruction(const TString& reason) && {
    Subscriber->OnFailure(reason);
}

TDuplicateFilterConstructor::TSourceIntervals::TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
    class TBorderView {
    private:
        using TReplaceKeyView = const NArrow::TReplaceKey*;
        YDB_READONLY_DEF(bool, IsLast);
        YDB_READONLY_DEF(TReplaceKeyView, PK);
        YDB_READONLY_DEF(ui64, SourceId);

        TBorderView(const bool isLast, const NArrow::TReplaceKey* const pk, const ui64 sourceId)
            : IsLast(isLast)
            , PK(pk)
            , SourceId(sourceId) {
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

    THashMap<ui64, ui32> firstBySourceId;
    for (const auto& border : borders) {
        if (border.GetIsLast()) {
            if (IntervalBorders.empty() || IntervalBorders.back() != *border.GetPK()) {
                IntervalBorders.emplace_back(*border.GetPK());
            }
            const TIntervalsRange sourceRange(
                *TValidator::CheckNotNull(firstBySourceId.FindPtr(border.GetSourceId())), IntervalBorders.size() - 1);
            AFL_VERIFY(SourceRanges.emplace(border.GetSourceId(), sourceRange).second);
        } else {
            AFL_VERIFY(firstBySourceId.emplace(border.GetSourceId(), IntervalBorders.size()).second);
        }
    }
    AFL_VERIFY(SourceRanges.size() == portions.size());
}

void TDuplicateFilterConstructor::Handle(const TEvRequestFilter::TPtr& ev) {
    // TODO: add counter: max volume of redundant memory for merges
    const auto& source = ev->Get()->GetSource();
    TIntervalsRange range = Intervals.GetRangeVerified(source->GetSourceId());
    AFL_VERIFY(AvailableSources.emplace(source->GetSourceId(), TSourceFilterConstructor(source, ev->Get()->GetSubscriber(), Intervals)).second);
    AvailableSourcesCount.AddRange(range.GetFirstIdx(), range.GetLastIdx(), source->GetSourceId());
    std::vector<ui32> readyIntervals = AwaitedSourcesCount.DecAndGetZeros(range.GetFirstIdx(), range.GetLastIdx());

    for (const ui32 intervalIdx : readyIntervals) {
        auto sourceIds = AvailableSourcesCount.FindIntersections(intervalIdx);
        AFL_VERIFY(sourceIds.size());
        const std::shared_ptr<NCommon::TSpecialReadContext> readContext =
            TValidator::CheckNotNull(AvailableSources.FindPtr(sourceIds.front()))->GetSource()->GetContext();
        const std::shared_ptr<TBuildDuplicateFilters> task =
            std::make_shared<TBuildDuplicateFilters>(readContext->GetReadMetadata()->GetReplaceKey(), IIndexInfo::GetSnapshotColumnNames());
        for (const ui64 sourceId : sourceIds) {
            const TSourceFilterConstructor* constructionInfo = AvailableSources.FindPtr(sourceId);
            AFL_VERIFY(constructionInfo)("source", sourceId);
            const std::shared_ptr<NCommon::IDataSource>& source = constructionInfo->GetSource();
            // TODO: why table has applied filter and not applied filter? which one to use?
            const auto intervalRange = constructionInfo->GetIntervalRange(intervalIdx);
            const auto slice =
                std::make_shared<NArrow::TGeneralContainer>(source->GetStageData()
                                                                .ToGeneralContainer(source->GetContext()->GetCommonContext()->GetResolver())
                                                                ->Slice(intervalRange.GetBegin(), intervalRange.Size()));
            task->AddSource(slice, source->GetStageData().GetNotAppliedFilter(),
                std::make_shared<TInternalFilterSubscriber>(intervalIdx, source->GetSourceId(), SelfId()));
        }
        NConveyor::TScanServiceOperator::SendTaskToExecute(task, readContext->GetCommonContext()->GetConveyorProcessId());
    }
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterPartialResult::TPtr& ev) {
    if (ev->Get()->GetResult().IsFail()) {
        AbortConstruction(ev->Get()->GetResult().GetErrorMessage());
        return;
    }
    TSourceFilterConstructor* constructor = AvailableSources.FindPtr(ev->Get()->GetSourceId());
    AFL_VERIFY(constructor)("portion", ev->Get()->GetSourceId());
    // TODO: avoid copying filters
    constructor->SetFilter(ev->Get()->GetIntervalIdx(), ev->Get()->ExtractResult().DetachResult());
    if (constructor->IsReady()) {
        std::move(*constructor).Finish();
        AFL_VERIFY(AvailableSources.erase(ev->Get()->GetSourceId()));
        if (AvailableSources.empty() && AwaitedSourcesCount.IsAllZeros()) {
            PassAway();
        }
    }
}

void TDuplicateFilterConstructor::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    AbortConstruction("aborted by actor system");
}

void TDuplicateFilterConstructor::AbortConstruction(const TString& reason) {
    for (auto&& [_, source] : AvailableSources) {
        std::move(source).AbortConstruction(reason);
    }
    PassAway();
}

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& portions)
    : TActor(&TDuplicateFilterConstructor::StateMain)
    , Intervals(portions)
    , AwaitedSourcesCount([this]() {
        std::vector<std::pair<ui32, ui32>> intervals;
        for (const auto& [_, interval] : Intervals.GetSourceRanges()) {
            intervals.emplace_back(interval.GetFirstIdx(), interval.GetLastIdx());
        }
        return intervals;
    }()) {
}

}   // namespace NKikimr::NOlap::NReader
