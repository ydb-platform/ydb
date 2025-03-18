#include "fetching.h"
#include "manager.h"
// TODO: move to simple_reader/

#include <ydb/core/tx/columnshard/engines/reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <bit>

namespace NKikimr::NOlap::NReader {

void TIntervalCounter::PropagateDelta(const TPosition& node) {
    if (PropagatedDeltas[node.GetIndex()]) {
        Count[node.GetIndex() * 2 + 1] += PropagatedDeltas[node.GetIndex()] * (node.IntervalSize() / 2);
        Count[node.GetIndex() * 2 + 2] += PropagatedDeltas[node.GetIndex()] * (node.IntervalSize() / 2);
        PropagatedDeltas[node.GetIndex()] = 0;
    }
}

void TIntervalCounter::Update(const TPosition& node, const TModification& modification) {
    if (modification.GetLeft() <= node.GetLeft() && modification.GetRight() >= node.GetRight()) {
        if (node.GetLeft() == node.GetRight()) {
            Count[node.GetIndex()] += modification.GetDelta();
        } else {
            PropagatedDeltas[node.GetIndex()] += modification.GetDelta();
        }
    } else {
        PropagateDelta(node.GetIndex());
        if (modification.GetLeft() <= node.LeftChild().GetRight()) {
            Update(node.LeftChild(), modification);
        }
        if (modification.GetRight() >= node.RightChild().GetLeft()) {
            Update(node.RightChild(), modification);
        }
    }
}

void TIntervalCounter::Inc(const ui32 l, const ui32 r) {
    Update(GetRoot(), TModification(l, r, 1));
}

ui64 TIntervalCounter::GetCount(const TPosition& node, const ui32 l, const ui32 r) const {
    if (l <= node.GetLeft() && r >= node.GetRight()) {
        return GetCount(node);
    }
    bool needLeft = node.LeftChild().GetRight() >= l;
    bool needRight = node.RightChild().GetLeft() <= r;
    AFL_VERIFY(needLeft || needRight);
    return (needLeft ? GetCount(node.LeftChild(), l, r) : 0) + (needRight ? GetCount(node.RightChild(), l, r) : 0);
}

TIntervalCounter::TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals) {
    ui32 maxValue = 0;
    for (const auto& [l, r] : intervals) {
        AFL_VERIFY(l <= r);
        if (r > maxValue) {
            maxValue = r;
        }
    }
    MaxIndex = std::bit_ceil(maxValue);
    Count.resize(MaxIndex * 2 + 1);
    PropagatedDeltas.resize(MaxIndex * 2 + 1);

    for (const auto& [l, r] : intervals) {
        Inc(l, r);
    }
}

bool TIntervalCounter::IsAllZeros() const {
    return GetCount(GetRoot()) == 0;
}

void TIntervalCounter::Dec(const ui32 l, const ui32 r) {
    return Update(GetRoot(), TModification(l, r, -1));
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter) {
    const ui32 localIdx = intervalIdx - FirstIntervalIdx;
    AFL_VERIFY(localIdx < IntervalFilters.size())("idx", localIdx)("size", IntervalFilters.size());
    AFL_VERIFY(!IntervalFilters[localIdx])("interval", intervalIdx);
    IntervalFilters[localIdx].emplace(std::move(filter));
    ++ReadyFilterCount;
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::Finish() {
    AFL_VERIFY(IsReady());
    NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildAllowFilter();
    for (ui64 i = 0; i < IntervalFilters.size(); ++i) {
        result.Append(*TValidator::CheckNotNull(IntervalFilters[i]));
    }
    AFL_VERIFY(result.GetRecordsCountVerified() == Source->GetStageData().GetTable()->GetRecordsCountVerified())(
                                                       "filter", result.GetRecordsCountVerified())("source", Source->GetRecordsCount());
    Subscriber->OnFilterReady(result);
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::AbortConstruction(const TString& reason) {
    if (Subscriber) {
        Subscriber->OnFailure(reason);
    }
}

TDuplicateFilterConstructor::TSourceIntervals::TSourceIntervals(const std::deque<std::shared_ptr<NSimple::IDataSource>>& sources) {
    for (const auto& source : sources) {
        SortedSourceIds.emplace_back(source->GetSourceId());
    }

    class TBorderView {
    private:
        YDB_READONLY_DEF(bool, IsLast);
        const NSimple::TReplaceKeyAdapter* PK;
        YDB_READONLY_DEF(ui64, SourceId);

        TBorderView(const bool isLast, const NSimple::TReplaceKeyAdapter* pk, const ui64 sourceId)
            : IsLast(isLast)
            , PK(pk)
            , SourceId(sourceId) {
        }

    public:
        static TBorderView First(const std::shared_ptr<NSimple::IDataSource>& source) {
            return TBorderView(false, &source->GetStart(), source->GetSourceId());
        }
        static TBorderView Last(const std::shared_ptr<NSimple::IDataSource>& source) {
            return TBorderView(true, &source->GetFinish(), source->GetSourceId());
        }

        bool operator<(const TBorderView& other) const {
            return std::tie<const NSimple::TReplaceKeyAdapter&, const bool&, const ui64&>(*PK, !IsLast, SourceId) <
                   std::tie<const NSimple::TReplaceKeyAdapter&, const bool&, const ui64&>(*other.PK, !other.IsLast, other.SourceId);
        };
        bool operator==(const TBorderView& other) const {
            return SourceId == other.SourceId && IsLast == other.IsLast;
        };

        const NSimple::TReplaceKeyAdapter& GetPK() const {
            return *PK;
        }
    };

    std::vector<TBorderView> borders;
    for (const auto& source : sources) {
        borders.emplace_back(TBorderView::First(source));
        borders.emplace_back(TBorderView::Last(source));
    }
    std::sort(borders.begin(), borders.end());

    THashMap<ui64, ui32> firstBySourceId;
    for (const auto& border : borders) {
        if (border.GetIsLast()) {
            if (IntervalBorders.empty() || IntervalBorders.back() != border.GetPK().GetReplaceKey()) {
                IntervalBorders.emplace_back(border.GetPK().GetReplaceKey());
            }
            const TIntervalsRange sourceRange(
                *TValidator::CheckNotNull(firstBySourceId.FindPtr(border.GetSourceId())), IntervalBorders.size() - 1);
            AFL_VERIFY(SourceRanges.emplace(border.GetSourceId(), sourceRange).second);
        } else {
            AFL_VERIFY(firstBySourceId.emplace(border.GetSourceId(), IntervalBorders.size()).second);
        }
    }
    AFL_VERIFY(SourceRanges.size() == sources.size());
}

void TDuplicateFilterConstructor::Handle(const TEvRequestFilter::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "request_duplicates_filter")("source_id", ev->Get()->GetSource()->GetSourceId());
    // TODO: handle step restarts (completely avoid them or fix verify here) Are they even possible (probably should not)?
    const ui32 sourceIdx = ev->Get()->GetSource()->GetSourceIdx();

    if (!SortedSources.empty() && sourceIdx == SortedSources.front()->GetSourceIdx()) {
        while (!SortedSources.empty() && Intervals.GetRangeBySourceId(SortedSources.front()->GetSourceId()).GetFirstIdx() <= Intervals.GetRangeBySourceIndex(sourceIdx).GetLastIdx()) {
            ActiveSources.emplace_back(std::make_shared<TSourceFilterConstructor>(SortedSources.front(), Intervals));
            SortedSources.pop_front();
            StartFetchingColumns(ActiveSources.back(), ev->Get()->GetSource()->GetMemoryGroupId());
        }
    }

    auto constructor = GetConstructorVerified(sourceIdx);
    constructor->SetSubscriber(ev->Get()->GetSubscriber());
    FlushFinishedSources();
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterPartialResult::TPtr& ev) {
    if (ev->Get()->GetResult().IsFail()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_merging_error")("error", ev->Get()->GetResult().GetErrorMessage());
        AbortConstruction(ev->Get()->GetResult().GetErrorMessage());
        return;
    }

    for (auto&& [sourceIdx, filter] : ev->Get()->DetachResult()) {
        AFL_VERIFY(sourceIdx >= FinishedSourcesCount);
        AFL_VERIFY(sourceIdx - FinishedSourcesCount < ActiveSources.size());
        std::shared_ptr<TSourceFilterConstructor> constructor = ActiveSources[sourceIdx - FinishedSourcesCount];
        // TODO: avoid copying filters
        constructor->SetFilter(ev->Get()->GetIntervalIdx(), std::move(filter));
    }

    while (!ActiveSources.empty() && ActiveSources.front()->IsReady()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "build_duplicates_filter")(
            "source_id", ActiveSources.front()->GetSource()->GetSourceId());
        ActiveSources.front()->Finish();
        ActiveSources.pop_front();
        ++FinishedSourcesCount;
    }

    FlushFinishedSources();
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterDataFetched::TPtr& ev) {
    if (ev->Get()->GetStatus().IsFail()) {
        AbortConstruction(ev->Get()->GetStatus().GetErrorMessage());
    }
    const TIntervalsRange intervals = Intervals.GetRangeBySourceId(ev->Get()->GetSourceId());
    NotFetchedSourcesCount.Dec(intervals.GetFirstIdx(), intervals.GetLastIdx());

    AFL_VERIFY(intervals.GetFirstIdx() >= NextIntervalToMerge.GetIntervalIdx());
    while (!NotFetchedSourcesCount.GetCount(NextIntervalToMerge.GetIntervalIdx(), NextIntervalToMerge.GetIntervalIdx())) {
        StartMergingColumns(NextIntervalToMerge);
        NextIntervalToMerge.Next();
    }
}

void TDuplicateFilterConstructor::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    AbortConstruction("aborted by actor system");
}

void TDuplicateFilterConstructor::AbortConstruction(const TString& reason) {
    for (auto&& source : std::move(ActiveSources)) {
        source->AbortConstruction(reason);
    }
    PassAway();
}

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const std::deque<std::shared_ptr<NSimple::IDataSource>>& sources)
    : TActor(&TDuplicateFilterConstructor::StateMain)
    , Intervals(sources)
    , SortedSources(sources)
    , NotFetchedSourcesCount([this]() {
        std::vector<std::pair<ui32, ui32>> intervals;
        for (const auto& [_, interval] : Intervals.GetSourceRanges()) {
            intervals.emplace_back(interval.GetFirstIdx(), interval.GetLastIdx());
        }
        return intervals;
    }())
    , NextIntervalToMerge(&Intervals) {
}

void TDuplicateFilterConstructor::StartFetchingColumns(const std::shared_ptr<TSourceFilterConstructor>& source, const ui64 memoryGroupId) const {
    auto fetchingContext = std::make_shared<TColumnFetchingContext>(
        source, source->GetSource()->GetContext()->GetCommonContext()->GetCounters().GetReadTasksGuard(), SelfId());

    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> fetchers;
    TReadActionsCollection readActions;
    const std::set<ui32> columnIds(
        fetchingContext->GetResultSchema()->GetColumnIds().begin(), fetchingContext->GetResultSchema()->GetColumnIds().end());
    AFL_VERIFY(!columnIds.empty());
    for (const ui32 columnId : columnIds) {
        std::shared_ptr<NCommon::IKernelFetchLogic> fetcher =
            std::make_shared<NCommon::TDefaultFetchLogic>(columnId, source->GetSource()->GetContext()->GetCommonContext()->GetStoragesManager());

        {
            NArrow::NAccessor::TAccessorsCollection emptyTable;
            NIndexes::TIndexesCollection emptyIndexes;
            NCommon::TFetchingResultContext contextFetch(emptyTable, emptyIndexes, source->GetSource());
            fetcher->Start(readActions, contextFetch);
        }
        fetchers.emplace(columnId, fetcher);
    }
    AFL_VERIFY(!readActions.IsEmpty());
    auto fetchingTask = std::make_shared<TColumnsFetcherTask>(std::move(readActions), fetchingContext);

    const ui64 mem = source->GetSource()->GetColumnsVolume(columnIds, NCommon::EMemType::Raw);
    auto allocationTask = std::make_shared<TColumnsMemoryAllocation>(mem, fetchingTask, fetchingContext);
    NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(source->GetSource()->GetContext()->GetProcessMemoryControlId(),
        source->GetSource()->GetContext()->GetCommonContext()->GetScanId(), memoryGroupId, { allocationTask },
        (ui32)NCommon::EStageFeaturesIndexes::Filter);
}

void TDuplicateFilterConstructor::StartMergingColumns(const TIntervalsCursor& interval) const {
    AFL_VERIFY(!ActiveSources.empty());
    const std::shared_ptr<NCommon::TSpecialReadContext> readContext = ActiveSources.front()->GetSource()->GetContext();
    const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
        readContext->GetReadMetadata()->GetReplaceKey(), IIndexInfo::GetSnapshotColumnNames(), interval.GetIntervalIdx(), SelfId());
    for (const auto& [_, sourceIdx] : interval.GetSourcesByRightInterval()) {
        auto constructionInfo = GetConstructorVerified(sourceIdx);
        const std::shared_ptr<NCommon::IDataSource>& source = constructionInfo->GetSource();
        const auto intervalRange = constructionInfo->GetIntervalRange(interval.GetIntervalIdx());
        const auto slice =
            std::make_shared<NArrow::TGeneralContainer>(source->GetStageData()
                                                            .ToGeneralContainer(source->GetContext()->GetCommonContext()->GetResolver())
                                                            ->Slice(intervalRange.GetBegin(), intervalRange.Size()));
        task->AddSource(slice, source->GetStageData().GetNotAppliedFilter(), source->GetSourceIdx());
    }
    NConveyor::TScanServiceOperator::SendTaskToExecute(task, readContext->GetCommonContext()->GetConveyorProcessId());
}

    void TDuplicateFilterConstructor::FlushFinishedSources() {
    if (ActiveSources.empty() && SortedSources.empty()) {
        AFL_VERIFY(NotFetchedSourcesCount.IsAllZeros());
        PassAway();
    }
    }

std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor> TDuplicateFilterConstructor::GetConstructorVerified(
    const ui32 sourceIdx) const {
    AFL_VERIFY(sourceIdx >= FinishedSourcesCount);
    AFL_VERIFY(sourceIdx - FinishedSourcesCount < ActiveSources.size());
    auto result = ActiveSources[sourceIdx - FinishedSourcesCount];
    AFL_VERIFY(result->GetSource()->GetSourceIdx() == sourceIdx);
    return result;
}

}   // namespace NKikimr::NOlap::NReader
