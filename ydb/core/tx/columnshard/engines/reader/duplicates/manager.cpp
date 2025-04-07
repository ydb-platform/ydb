#include "fetching.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/conveyor/usage/service.h>

#include <bit>

namespace NKikimr::NOlap::NReader {

void TIntervalCounter::PropagateDelta(const TPosition& node, TZeroCollector* callback) {
    AFL_VERIFY(node.GetIndex() < PropagatedDeltas.size());
    if (!PropagatedDeltas[node.GetIndex()]) {
        return;
    }

    const ui64 left = node.GetIndex() * 2 + 1;
    const ui64 right = node.GetIndex() * 2 + 2;
    if (left < PropagatedDeltas.size()) {
        AFL_VERIFY(right < PropagatedDeltas.size());
        PropagatedDeltas[left] += PropagatedDeltas[node.GetIndex()];
        PropagatedDeltas[right] += PropagatedDeltas[node.GetIndex()];
    } else {
        AFL_VERIFY((i64)Count[left] >= -PropagatedDeltas[node.GetIndex()]);
        AFL_VERIFY((i64)Count[right] >= -PropagatedDeltas[node.GetIndex()]);
        Count[left] += PropagatedDeltas[node.GetIndex()];
        Count[right] += PropagatedDeltas[node.GetIndex()];
    }
    const i64 delta = PropagatedDeltas[node.GetIndex()] * (node.IntervalSize());
    AFL_VERIFY((i64)Count[node.GetIndex()] >= -delta);
    Count[node.GetIndex()] += delta;
    MinValue[node.GetIndex()] += PropagatedDeltas[node.GetIndex()];
    PropagatedDeltas[node.GetIndex()] = 0;

    for (const auto& child : { node.LeftChild(), node.RightChild() }) {
        if (!GetCount(child)) {
            AFL_VERIFY(callback);
            callback->OnNewZeros(child);
        } else if (!GetMinValue(child)) {
            PropagateDelta(child, callback);
        }
    }
}

void TIntervalCounter::Update(const TPosition& node, const TModification& modification, TZeroCollector* callback) {
    AFL_VERIFY(modification.GetDelta());
    if (modification.GetLeft() <= node.GetLeft() && modification.GetRight() >= node.GetRight()) {
        if (node.GetLeft() == node.GetRight()) {
            AFL_VERIFY((i64)Count[node.GetIndex()] >= -modification.GetDelta());
            Count[node.GetIndex()] += modification.GetDelta();
            MinValue[node.GetIndex()] += modification.GetDelta();
            if (!GetCount(node)) {
                AFL_VERIFY(callback);
                callback->OnNewZeros(node);
            }
        } else {
            AFL_VERIFY(node.GetIndex() < PropagatedDeltas.size());
            PropagatedDeltas[node.GetIndex()] += modification.GetDelta();
            if (GetMinValue(node) == 0 && callback) {
                PropagateDelta(node, callback);
            }
        }
    } else {
        PropagateDelta(node, callback);
        if (modification.GetLeft() <= node.LeftChild().GetRight()) {
            Update(node.LeftChild(), modification, callback);
        }
        if (modification.GetRight() >= node.RightChild().GetLeft()) {
            Update(node.RightChild(), modification, callback);
        }
        Count[node.GetIndex()] = GetCount(node.LeftChild()) + GetCount(node.RightChild());
        MinValue[node.GetIndex()] = Min(GetMinValue(node.LeftChild()), GetMinValue(node.RightChild()));
    }
}

void TIntervalCounter::Inc(const ui32 l, const ui32 r) {
    Update(GetRoot(), TModification(l, r, 1), nullptr);
}

ui64 TIntervalCounter::GetCount(const TPosition& node, const ui32 l, const ui32 r) {
    if (l <= node.GetLeft() && r >= node.GetRight()) {
        return GetCount(node);
    }
    PropagateDelta(node, nullptr);
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
    if (maxValue == std::bit_ceil(maxValue)) {
        MaxIndex = maxValue * 2 - 1;
    } else {
        MaxIndex = std::bit_ceil(maxValue) - 1;
    }
    Count.resize(MaxIndex * 2 + 1);
    MinValue.resize(Count.size());
    PropagatedDeltas.resize(MaxIndex);

    for (const auto& [l, r] : intervals) {
        Inc(l, r);
    }
}

bool TIntervalCounter::IsAllZeros() const {
    return GetCount(GetRoot()) == 0;
}

std::vector<ui32> TIntervalCounter::DecAndGetZeros(const ui32 l, const ui32 r) {
    TZeroCollector callback;
    Update(GetRoot(), TModification(l, r, -1), &callback);
    return callback.ExtractNewZeroPositions();
}

TDuplicateFilterConstructor::TSourceFilterConstructor::TSourceFilterConstructor(
    const std::shared_ptr<NSimple::TPortionDataSource>& source, const TSourceIntervals& intervals)
    : FirstIntervalIdx(intervals.GetRangeBySourceId(source->GetSourceId()).GetFirstIdx())
    , IntervalFilters(intervals.GetRangeBySourceId(source->GetSourceId()).NumIntervals())
    , Source(source) {
    AFL_VERIFY(Source);
    AFL_VERIFY(IntervalFilters.size());

    const TIntervalsRange range = intervals.GetRangeBySourceId(Source->GetSourceId());
    RightIntervalBorders.reserve(range.NumIntervals());
    for (ui32 intervalIdx = range.GetFirstIdx(); intervalIdx <= range.GetLastIdx(); ++intervalIdx) {
        RightIntervalBorders.emplace_back(intervals.GetRightInclusiveBorder(intervalIdx));
    }
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::SetColumnData(std::shared_ptr<NArrow::TGeneralContainer>&& data) {
    AFL_VERIFY(MemoryGuard);
    AFL_VERIFY(!ColumnData);
    ColumnData = std::move(data);

    AFL_VERIFY(ColumnData);
    IntervalOffsets.emplace_back(0);
    for (ui32 localIntervalIdx = 1; localIntervalIdx < RightIntervalBorders.size(); ++localIntervalIdx) {
        NArrow::TGeneralContainer keysData = [this]() {
            // TODO: optimize?
            // TODO: simplify?
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
            const auto& pkSchema = Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey();
            for (const auto& field : pkSchema->fields()) {
                columns.emplace_back(ColumnData->GetAccessorByNameOptional(field->name()));
            }
            return NArrow::TGeneralContainer(Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey(), std::move(columns));
        }();
        const NArrow::NAccessor::IChunkedArray::TRowRange findLeftBorder = keysData.EqualRange(
            *RightIntervalBorders[localIntervalIdx - 1].ToBatch(Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey()));
        IntervalOffsets.emplace_back(findLeftBorder.GetEnd());
    }
    AFL_VERIFY(IntervalOffsets.size() == IntervalFilters.size());
}

NArrow::NAccessor::IChunkedArray::TRowRange TDuplicateFilterConstructor::TSourceFilterConstructor::GetIntervalRange(
    const ui32 globalIntervalIdx) const {
    AFL_VERIFY(!IntervalOffsets.empty());
    AFL_VERIFY(globalIntervalIdx >= FirstIntervalIdx)("global", globalIntervalIdx)("first", FirstIntervalIdx);
    const ui32 localIntervalIdx = globalIntervalIdx - FirstIntervalIdx;
    AFL_VERIFY(localIntervalIdx < IntervalOffsets.size())("local", localIntervalIdx)("global", globalIntervalIdx)(
                                      "size", IntervalOffsets.size());
    const NArrow::NAccessor::IChunkedArray::TRowRange localRange =
        (localIntervalIdx == IntervalOffsets.size() - 1)
            ? NArrow::NAccessor::IChunkedArray::TRowRange(IntervalOffsets[localIntervalIdx], Source->GetRecordsCount())
            : NArrow::NAccessor::IChunkedArray::TRowRange(IntervalOffsets[localIntervalIdx], IntervalOffsets[localIntervalIdx + 1]);
    return localRange;
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter) {
    AFL_VERIFY(filter.GetRecordsCount().value_or(0) == GetIntervalRange(intervalIdx).Size())("actual", filter.GetRecordsCount().value_or(0))(
                                                           "expected", GetIntervalRange(intervalIdx).Size());
    const ui32 localIdx = intervalIdx - FirstIntervalIdx;
    AFL_VERIFY(localIdx < IntervalFilters.size())("idx", localIdx)("size", IntervalFilters.size());
    AFL_VERIFY(!IntervalFilters[localIdx])("interval", intervalIdx);
    IntervalFilters[localIdx].emplace(std::move(filter));
    ++ReadyFilterCount;
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::Finish() {
    AFL_VERIFY(IsReady());
    NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildAllowFilter();
    auto appendFilter = [&result, this](const ui64 i) {
        result.Append(*TValidator::CheckNotNull(IntervalFilters[i]));
    };

    for (ui64 i = 0; i < IntervalFilters.size(); ++i) {
        appendFilter(i);
    }

    AFL_VERIFY(result.GetRecordsCountVerified() == Source->GetRecordsCount())("filter", result.GetRecordsCountVerified())(
                                                       "source", Source->GetRecordsCount());
    Subscriber->OnFilterReady(result);
}

void TDuplicateFilterConstructor::TSourceFilterConstructor::AbortConstruction(const TString& reason) {
    if (Subscriber) {
        Subscriber->OnFailure(reason);
    }
}

std::shared_ptr<NSimple::TPortionDataSource> TDuplicateFilterConstructor::TWaitingSourceInfo::Construct(
    const std::shared_ptr<NSimple::TSpecialReadContext>& context) const {
    const auto& portions = context->GetReadMetadata()->SelectInfo->Portions;
    AFL_VERIFY(PortionIdx < portions.size());
    return std::make_shared<NSimple::TPortionDataSource>(PortionIdx, portions[PortionIdx], context);
}

TDuplicateFilterConstructor::TSourceIntervals::TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& sources) {
    for (const auto& source : sources) {
        SortedSourceIds.emplace_back(source->GetPortionId());
    }

    class TBorderView {
    private:
        YDB_READONLY_DEF(bool, IsLast);
        const NArrow::TReplaceKey* Start;
        YDB_READONLY_DEF(ui64, SourceId);

        TBorderView(const bool isLast, const NArrow::TReplaceKey* pk, const ui64 sourceId)
            : IsLast(isLast)
            , Start(pk)
            , SourceId(sourceId) {
        }

    public:
        static TBorderView First(const std::shared_ptr<TPortionInfo>& source) {
            return TBorderView(false, &source->IndexKeyStart(), source->GetPortionId());
        }
        static TBorderView Last(const std::shared_ptr<TPortionInfo>& source) {
            return TBorderView(true, &source->IndexKeyEnd(), source->GetPortionId());
        }

        bool operator<(const TBorderView& other) const {
            return std::tie<const NArrow::TReplaceKey&, const bool&, const ui64&>(*Start, IsLast, SourceId) <
                   std::tie<const NArrow::TReplaceKey&, const bool&, const ui64&>(*other.Start, other.IsLast, other.SourceId);
        };
        bool operator==(const TBorderView& other) const {
            return SourceId == other.SourceId && IsLast == other.IsLast;
        };

        const NArrow::TReplaceKey& GetStart() const {
            return *Start;
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
            if (IntervalBorders.empty() || IntervalBorders.back() != border.GetStart()) {
                IntervalBorders.emplace_back(border.GetStart());
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
    const ui64 sourceId = ev->Get()->GetSource()->GetSourceId();
    const auto range = Intervals.GetRangeBySourceId(sourceId);

    const THashSet<ui64> notFetchedSources = NotFetchedSourcesIndex.GetPartialIntersections(range.GetFirstIdx(), range.GetLastIdx());
    for (const auto& sourceId : notFetchedSources) {
        StartAllocation(sourceId, ev->Get()->GetSource());
    }

    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "request_filter")("source", sourceId)(
        "range", range.DebugString())("fetching", TStringBuilder() << '[' << JoinSeq(',', notFetchedSources) << ']');

    auto* findConstructor = ActiveSources.FindPtr(sourceId);
    AFL_VERIFY(findConstructor);
    (*findConstructor)->SetSubscriber(ev->Get()->GetSubscriber());
    if ((*findConstructor)->IsReady()) {
        (*findConstructor)->Finish();
        ActiveSources.erase(sourceId);
    }
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterIntervalResult::TPtr& ev) {
    if (ev->Get()->GetResult().IsFail()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_merging_error")("error", ev->Get()->GetResult().GetErrorMessage());
        AbortAndPassAway(ev->Get()->GetResult().GetErrorMessage());
        return;
    }

    for (auto&& [sourceId, filter] : ev->Get()->DetachResult()) {
        auto* findConstructor = ActiveSources.FindPtr(sourceId);
        AFL_VERIFY(findConstructor);
        (*findConstructor)->SetFilter(ev->Get()->GetIntervalIdx(), std::move(filter));
        if ((*findConstructor)->IsReady()) {
            (*findConstructor)->Finish();
            ActiveSources.erase(sourceId);
        }
    }
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterStartFetching::TPtr& ev) {
    const ui64 sourceId = ev->Get()->GetContext()->GetSourceId();

    AFL_VERIFY(WaitingSources.erase(sourceId));
    auto* findConstructor = ActiveSources.FindPtr(sourceId);
    AFL_VERIFY(findConstructor);
    ev->Get()->GetContext()->SetConstructor(*findConstructor);
    ev->Get()->GetContext()->ContinueFetching(ev->Get()->GetNextAction());
}

void TDuplicateFilterConstructor::Handle(const TEvDuplicateFilterDataFetched::TPtr& ev) {
    if (ev->Get()->GetStatus().IsFail()) {
        AbortAndPassAway(ev->Get()->GetStatus().GetErrorMessage());
        return;
    }

    const TIntervalsRange intervals = Intervals.GetRangeBySourceId(ev->Get()->GetSourceId());
    for (ui32 i = intervals.GetFirstIdx(); i <= intervals.GetLastIdx(); ++i) {
        FetchedSourcesByInterval[i].emplace_back(ev->Get()->GetSourceId());
    }

    auto readyIntervals = NotFetchedSourcesCount.DecAndGetZeros(intervals.GetFirstIdx(), intervals.GetLastIdx());
    for (const ui32 interval : readyIntervals) {
        StartMergingColumns(interval);
    }
}

void TDuplicateFilterConstructor::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    AbortAndPassAway("aborted by actor system");
}

void TDuplicateFilterConstructor::AbortAndPassAway(const TString& reason) {
    for (auto [id, constructor] : ActiveSources) {
        constructor->AbortConstruction(reason);
    }
    ActiveSources.clear();
    PassAway();
}

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const NSimple::TSpecialReadContext& context)
    : TActor(&TDuplicateFilterConstructor::StateMain)
    , Intervals(context.GetReadMetadata()->SelectInfo->Portions)
    , NotFetchedSourcesCount([this]() {
        std::vector<std::pair<ui32, ui32>> intervals;
        for (const auto& [_, interval] : Intervals.GetSourceRanges()) {
            intervals.emplace_back(interval.GetFirstIdx(), interval.GetLastIdx());
        }
        return intervals;
    }()) {
    for (const auto& [id, range] : Intervals.GetSourceRanges()) {
        NotFetchedSourcesIndex.Insert(range.GetFirstIdx(), range.GetLastIdx(), id);
    }
    const auto& portions = context.GetReadMetadata()->SelectInfo->Portions;
    for (ui64 i = 0; i < portions.size(); ++i) {
        WaitingSources.emplace(portions[i]->GetPortionId(), std::make_shared<TWaitingSourceInfo>(i));
    }
}

void TDuplicateFilterConstructor::StartAllocation(const ui64 sourceId, const std::shared_ptr<NSimple::IDataSource>& requester) {
    const ui64 memoryGroupId = requester->GetMemoryGroupId();
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "start_fetching_columns")("source", sourceId)(
        "memory_group", memoryGroupId);

    auto findWaiting = WaitingSources.FindPtr(sourceId);
    if (!findWaiting) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "skip_start_allocation")(
            "reason", "already_allocated")("source", sourceId)("memory_group", memoryGroupId);
        return;
    }
    if (!(*findWaiting)->SetStartAllocation(memoryGroupId)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "skip_start_allocation")(
            "reason", "allocation_started")("source", sourceId)("memory_group", memoryGroupId);
        return;
    }

    std::shared_ptr<NSimple::TPortionDataSource> source =
        (*findWaiting)->Construct(requester->GetContextAsVerified<NSimple::TSpecialReadContext>());
    auto findConstructor = ActiveSources.FindPtr(sourceId);
    if (!findConstructor) {
        findConstructor = &ActiveSources.emplace(sourceId, std::make_shared<TSourceFilterConstructor>(source, Intervals)).first->second;
    }
    std::shared_ptr<TColumnFetchingContext> fetchingContext = std::make_shared<TColumnFetchingContext>(
        source->GetSourceId(), *findWaiting, SelfId(), source->GetContextAsVerified<NSimple::TSpecialReadContext>(), requester->GetGroupGuard());
    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("DUPLICATES");
    request->AddPortion(source->GetPortionInfoPtr());
    request->SetColumnIds(
        { fetchingContext->GetResultSchema()->GetColumnIds().begin(), fetchingContext->GetResultSchema()->GetColumnIds().end() });
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(fetchingContext));

    source->GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
}

void TDuplicateFilterConstructor::StartMergingColumns(const ui32 intervalIdx) {
    auto findSources = FetchedSourcesByInterval.find(intervalIdx);
    AFL_VERIFY(findSources != FetchedSourcesByInterval.end());
    std::vector<ui64> sources = std::move(findSources->second);
    FetchedSourcesByInterval.erase(findSources);

    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "start_merging_columns")(
        "columns", sources.size())("interval", intervalIdx);

    AFL_VERIFY(!ActiveSources.empty());
    const std::shared_ptr<NCommon::TSpecialReadContext> readContext = ActiveSources.begin()->second->GetSource()->GetContext();
    const std::shared_ptr<TBuildDuplicateFilters> task =
        std::make_shared<TBuildDuplicateFilters>(readContext->GetReadMetadata()->GetReplaceKey(), IIndexInfo::GetSnapshotColumnNames(),
            intervalIdx, SelfId(), readContext->GetCommonContext()->GetCounters());

    std::vector<ui64> emptySources;
    std::vector<ui64> nonEmptySources;

    for (const auto& sourceId : sources) {
        const auto* findSource = ActiveSources.FindPtr(sourceId);
        AFL_VERIFY(findSource);
        std::shared_ptr<TSourceFilterConstructor> constructionInfo = *findSource;
        const NArrow::NAccessor::IChunkedArray::TRowRange range = constructionInfo->GetIntervalRange(intervalIdx);
        if (range.Empty()) {
            emptySources.emplace_back(constructionInfo->GetSource()->GetSourceId());
            continue;
        }

        nonEmptySources.emplace_back(constructionInfo->GetSource()->GetSourceId());
        std::shared_ptr<NArrow::TGeneralContainer> slice =
            range.Empty()
                ? constructionInfo->GetColumnData()->BuildEmptySame()
                : std::make_shared<NArrow::TGeneralContainer>(constructionInfo->GetColumnData()->Slice(range.GetBegin(), range.Size()));
        task->AddSource(std::move(slice), std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter()),
            constructionInfo->GetSource()->GetSourceId());
    }

    if (emptySources.size()) {
        THashMap<ui64, NArrow::TColumnFilter> emptyFilters;
        for (const auto& sourceId : emptySources) {
            emptyFilters.emplace(sourceId, NArrow::TColumnFilter::BuildAllowFilter());
        }
        Send(SelfId(), new TEvDuplicateFilterIntervalResult(std::move(emptyFilters), intervalIdx));
    }

    if (nonEmptySources.size() > 1) {
        NConveyor::TScanServiceOperator::SendTaskToExecute(task, readContext->GetCommonContext()->GetConveyorProcessId());
    } else if (nonEmptySources.size() == 1) {
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        const ui64 numEntries =
            (*TValidator::CheckNotNull(ActiveSources.FindPtr(nonEmptySources.front())))->GetIntervalRange(intervalIdx).Size();
        filter.Add(true, numEntries);
        readContext->GetCommonContext()->GetCounters().OnRowsMerged(0, 0, numEntries);
        Send(SelfId(), new TEvDuplicateFilterIntervalResult(
                           THashMap<ui64, NArrow::TColumnFilter>({ { nonEmptySources.front(), std::move(filter) } }), intervalIdx));
    }
}

}   // namespace NKikimr::NOlap::NReader
