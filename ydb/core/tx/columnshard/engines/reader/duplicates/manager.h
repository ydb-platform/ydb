#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader {

namespace NSimple {
class IDataSource;
}

class TEvNotifyReadingFinished: public NActors::TEventLocal<TEvNotifyReadingFinished, NColumnShard::TEvPrivate::EvNotifyReadingFinished> {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NCommon::IDataSource>>, Sources);

public:
    TEvNotifyReadingFinished(const std::vector<std::shared_ptr<NCommon::IDataSource>>& sources)
        : Sources(sources) {
        AFL_VERIFY(!sources.empty());
    }
};

class TEvDuplicateFilterIntervalResult
    : public NActors::TEventLocal<TEvDuplicateFilterIntervalResult, NColumnShard::TEvPrivate::EvDuplicateFilterIntervalResult> {
private:
    using TFilterBySourceId = THashMap<ui64, NArrow::TColumnFilter>;
    YDB_READONLY(TConclusion<TFilterBySourceId>, Result, TFilterBySourceId());
    YDB_READONLY_DEF(ui32, IntervalIdx);

public:
    TEvDuplicateFilterIntervalResult(TConclusion<TFilterBySourceId>&& result, const ui32 intervalIdx)
        : Result(std::move(result))
        , IntervalIdx(intervalIdx) {
        AFL_VERIFY(!Result->empty());
    }

    TFilterBySourceId&& DetachResult() {
        return Result.DetachResult();
    }
};

class TEvDuplicateFilterDataFetched
    : public NActors::TEventLocal<TEvDuplicateFilterDataFetched, NColumnShard::TEvPrivate::EvDuplicateFilterDataFetched> {
private:
    YDB_READONLY_DEF(ui64, SourceId);
    YDB_READONLY(TConclusionStatus, Status, TConclusionStatus::Success());

public:
    TEvDuplicateFilterDataFetched(const ui64 sourceId, const TConclusionStatus& status)
        : SourceId(sourceId)
        , Status(status) {
    }
};

class TIntervalCounter {
private:
    class TModification {
    private:
        YDB_READONLY_DEF(ui32, Left);
        YDB_READONLY_DEF(ui32, Right);
        YDB_READONLY_DEF(i64, Delta);

    public:
        TModification(const ui32 l, const ui32 r, const i64 delta)
            : Left(l)
            , Right(r)
            , Delta(delta) {
        }
    };

    class TPosition {
    private:
        YDB_READONLY_DEF(ui32, Index);
        YDB_READONLY_DEF(ui32, Left);
        YDB_READONLY_DEF(ui32, Right);

        TPosition(const ui32 i, const ui32 l, const ui32 r)
            : Index(i)
            , Left(l)
            , Right(r) {
        }

    public:
        explicit TPosition(const ui32 maxIndex)
            : Index(0)
            , Left(0)
            , Right(maxIndex) {
        }

        TPosition LeftChild() const {
            AFL_VERIFY(Right > Left);
            return TPosition(Index * 2 + 1, Left, (Left + Right - 1) / 2);
        }

        TPosition RightChild() const {
            AFL_VERIFY(Right > Left);
            return TPosition(Index * 2 + 2, (Left + Right + 1) / 2, Right);
        }

        ui32 IntervalSize() const {
            return Right - Left + 1;
        }
    };

    // Segment tree: Count[i] = GetCount(i * 2 + 1) + GetCount(i * 2 + 2)
    std::vector<ui64> Count;
    std::vector<i64> PropagatedDeltas;
    ui32 MaxIndex = 0;

private:
    void PropagateDelta(const TPosition& node);
    void Update(const TPosition& node, const TModification& modification);
    void Inc(const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node, const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node) const {
        AFL_VERIFY(node.GetIndex() < Count.size());
        return Count[node.GetIndex()] +
               (node.GetIndex() < PropagatedDeltas.size() ? PropagatedDeltas[node.GetIndex()] : 0) * node.IntervalSize();
    }
    TPosition GetRoot() const {
        return TPosition(MaxIndex);
    }

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);
    void Dec(const ui32 l, const ui32 r);
    ui64 GetCount(const ui32 l, const ui32 r) {
        return GetCount(GetRoot(), l, r);
    }
    bool IsAllZeros() const;
};

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
    friend class TColumnFetchingContext;

    class TNoOpSubscriber: public IFilterSubscriber {
    private:
        virtual void OnFilterReady(const NArrow::TColumnFilter&) {
        }
        virtual void OnFailure(const TString&) {
        }
    };

    class TIntervalsRange {
    private:
        YDB_READONLY_DEF(ui32, FirstIdx);
        YDB_READONLY_DEF(ui32, LastIdx);

    public:
        TIntervalsRange(const ui32 first, ui32 last)
            : FirstIdx(first)
            , LastIdx(last) {
            AFL_VERIFY(last >= first)("first", first)("last", last);
        }

        ui32 NumIntervals() const {
            return LastIdx - FirstIdx + 1;
        }
    };

    class TSourceIntervals {
    private:
        using TRangeBySourceId = THashMap<ui64, TIntervalsRange>;
        YDB_READONLY_DEF(TRangeBySourceId, SourceRanges);
        YDB_READONLY_DEF(std::vector<ui64>, SortedSourceIds);
        std::vector<NArrow::TReplaceKey> IntervalBorders;

    public:
        TSourceIntervals(const std::deque<std::shared_ptr<NSimple::IDataSource>>& sources);

        const NArrow::TReplaceKey& GetRightInclusiveBorder(const ui32 intervalIdx) const {
            return IntervalBorders[intervalIdx];
        }
        const std::optional<NArrow::TReplaceKey> GetLeftExlusiveBorder(const ui32 intervalIdx) const {
            if (intervalIdx == 0) {
                return std::nullopt;
            }
            return IntervalBorders[intervalIdx - 1];
        }
        ui32 NumIntervals() const {
            return IntervalBorders.size();
        }
        ui32 NumSources() const {
            return SourceRanges.size();
        }

        TIntervalsRange GetRangeBySourceId(const ui64 sourceId) const {
            const TIntervalsRange* findRange = SourceRanges.FindPtr(sourceId);
            AFL_VERIFY(findRange)("source", sourceId);
            return *findRange;
        }

        TIntervalsRange GetRangeBySourceSeqNumber(const ui32 seqNumber) const {
            AFL_VERIFY(seqNumber < SortedSourceIds.size());
            return GetRangeBySourceId(SortedSourceIds[seqNumber]);
        }
    };

    class TIntervalsCursor {
    private:
        using TSourceSeqNumberByIntervalIdx = std::multimap<ui32, ui32>;
        YDB_READONLY(ui32, IntervalIdx, 0);
        YDB_READONLY_DEF(TSourceSeqNumberByIntervalIdx, SourcesByRightInterval);
        const TSourceIntervals* Owner;
        ui32 NextSourceSeqNumber = 0;

        void NextImpl(const bool init = false) {
            if (init) {
                AFL_VERIFY(IntervalIdx == 0);
            } else {
                ++IntervalIdx;
            }

            while (!SourcesByRightInterval.empty() && SourcesByRightInterval.begin()->first < IntervalIdx) {
                SourcesByRightInterval.erase(SourcesByRightInterval.begin());
            }

            while (NextSourceSeqNumber != Owner->NumSources() && Owner->GetRangeBySourceSeqNumber(NextSourceSeqNumber).GetFirstIdx() <= IntervalIdx) {
                SourcesByRightInterval.emplace(Owner->GetRangeBySourceSeqNumber(NextSourceSeqNumber).GetLastIdx(), NextSourceSeqNumber);
                ++NextSourceSeqNumber;
            }

            AFL_VERIFY(IntervalIdx <= Owner->NumIntervals())("i", IntervalIdx)("num_intervals", Owner->NumIntervals());
            AFL_VERIFY(IsEnd() == SourcesByRightInterval.empty())("sources", SourcesByRightInterval.size())("interval", IntervalIdx);
        }

    public:
        TIntervalsCursor(const TSourceIntervals* owner)
            : Owner(owner) {
            AFL_VERIFY(Owner);
            NextImpl(true);
        }

        void Next() {
            NextImpl(false);
        }

        bool IsEnd() const {
            return IntervalIdx == Owner->NumIntervals();
        }
    };

    class TSourceFilterConstructor {
    private:
        ui32 FirstIntervalIdx;
        YDB_READONLY_DEF(std::vector<std::optional<NArrow::TColumnFilter>>, IntervalFilters);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, ColumnData);
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> MemoryGuard;
        YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
        std::shared_ptr<IFilterSubscriber> Subscriber;
        std::vector<ui64> IntervalOffsets;
        std::vector<NArrow::TReplaceKey> RightIntervalBorders;
        ui64 ReadyFilterCount = 0;

    public:
        TSourceFilterConstructor(const std::shared_ptr<NCommon::IDataSource>& source, const TSourceIntervals& intervals)
            : FirstIntervalIdx(intervals.GetRangeBySourceId(source->GetSourceId()).GetFirstIdx())
            , IntervalFilters(intervals.GetRangeBySourceId(source->GetSourceId()).NumIntervals())
            , Source(source) {
            const TIntervalsRange range = intervals.GetRangeBySourceId(Source->GetSourceId());
            RightIntervalBorders.reserve(range.NumIntervals());
            for (ui32 intervalIdx = range.GetFirstIdx(); intervalIdx <= range.GetLastIdx(); ++intervalIdx) {
                RightIntervalBorders.emplace_back(intervals.GetRightInclusiveBorder(intervalIdx));
            }
        }

        void StartFetchingColumns(const ui64 memoryGroupId);

        void SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter);
        void SetMemoryGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
            AFL_VERIFY(!MemoryGuard);
            MemoryGuard = std::move(guard);
        }
        void SetColumnData(std::shared_ptr<NArrow::TGeneralContainer>&& data) {
            AFL_VERIFY(MemoryGuard);
            AFL_VERIFY(!ColumnData);
            ColumnData = std::move(data);

            AFL_VERIFY(ColumnData);
            IntervalOffsets.emplace_back(0);
            for (ui32 intervalIdx = FirstIntervalIdx + 1; intervalIdx < FirstIntervalIdx + IntervalFilters.size(); ++intervalIdx) {
                NArrow::TGeneralContainer keysData = [this]() {
                    // TODO: optimize?
                    // TODO: simplify?
                    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
                    const auto& pkSchema = Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey();
                    for (const auto& field : pkSchema->fields()) {
                        columns.emplace_back(ColumnData->GetAccessorByNameOptional(field->name()));
                    }
                    return NArrow::TGeneralContainer(
                        Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey(), std::move(columns));
                }();
                const NArrow::NAccessor::IChunkedArray::TRowRange findLeftBorder = keysData.EqualRange(
                    *RightIntervalBorders[intervalIdx - 1].ToBatch(Source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey()));
                // TODO: hide decision in a class?
                IntervalOffsets.emplace_back(Source->GetContext()->GetReadMetadata()->IsDescSorted()
                                                 ? ColumnData->GetRecordsCount() - findLeftBorder.GetBegin()
                                                 : findLeftBorder.GetEnd());
            }
            AFL_VERIFY(IntervalOffsets.size() == IntervalFilters.size());
        }
        void SetSubscriber(const std::shared_ptr<IFilterSubscriber>& subscriber) {
            AFL_VERIFY(!Subscriber);
            Subscriber = subscriber;
        }

        bool IsReady() const {
            return Subscriber && ReadyFilterCount == IntervalFilters.size();
        }

        NArrow::NAccessor::IChunkedArray::TRowRange GetIntervalRange(const ui32 globalIntervalIdx) const {
            AFL_VERIFY(!IntervalOffsets.empty());
            AFL_VERIFY(globalIntervalIdx >= FirstIntervalIdx)("global", globalIntervalIdx)("first", FirstIntervalIdx);
            const ui32 localIntervalIdx = globalIntervalIdx - FirstIntervalIdx;
            AFL_VERIFY(localIntervalIdx < IntervalOffsets.size())("local", localIntervalIdx)("global", globalIntervalIdx)(
                                              "size", IntervalOffsets.size());
            const NArrow::NAccessor::IChunkedArray::TRowRange localRange =
                (localIntervalIdx == IntervalOffsets.size() - 1)
                    ? NArrow::NAccessor::IChunkedArray::TRowRange(IntervalOffsets[localIntervalIdx], Source->GetRecordsCount())
                    : NArrow::NAccessor::IChunkedArray::TRowRange(IntervalOffsets[localIntervalIdx], IntervalOffsets[localIntervalIdx + 1]);
            if (Source->GetContext()->GetReadMetadata()->IsDescSorted()) {
                return { Source->GetRecordsCount() - localRange.GetEnd(), Source->GetRecordsCount() - localRange.GetBegin() };
            } else {
                return localRange;
            }
        }

        void Finish();
        void AbortConstruction(const TString& reason);
    };

    class IAction {
    public:
        virtual void Execute(TDuplicateFilterConstructor& self, const ui64 sourceId) = 0;
        virtual ~IAction() = default;
    };

    class TRequestQueue {
    private:
        std::deque<ui64> OrderedSourceIds;
        THashMap<ui64, std::unique_ptr<IAction>> RequestsBySourceId;

    public:
        void AddRequest(const ui64 sourceId, std::unique_ptr<IAction> action) {
            AFL_VERIFY(RequestsBySourceId.emplace(sourceId, std::move(action)).second);
        }

        [[nodiscard]] std::optional<std::pair<ui64, std::unique_ptr<IAction>>> ExtractNextAction() {
            if (OrderedSourceIds.empty()) {
                return std::nullopt;
            }
            auto findNextRequest = RequestsBySourceId.find(OrderedSourceIds.front());
            if (findNextRequest == RequestsBySourceId.end()) {
                return std::nullopt;
            }
            OrderedSourceIds.pop_front();
            std::pair<ui64, std::unique_ptr<IAction>> result(findNextRequest->first, std::move(findNextRequest->second));
            RequestsBySourceId.erase(findNextRequest);
            return result;
        }

        bool IsDone() const {
            return OrderedSourceIds.empty();
        }

        TRequestQueue(const TSourceIntervals& intervals)
            : OrderedSourceIds(intervals.GetSortedSourceIds().begin(), intervals.GetSortedSourceIds().end()) {
        }
    };

    class TActionSkip: public IAction {
    private:
        virtual void Execute(TDuplicateFilterConstructor& self, const ui64 sourceId) override {
            self.DoSkip(sourceId);
        }
    };

    class TActionGetFilter: public IAction {
    private:
        std::shared_ptr<IFilterSubscriber> Subscriber;
        ui64 MemoryGroupId;

        virtual void Execute(TDuplicateFilterConstructor& self, const ui64 sourceId) override {
            self.DoGetFilter(sourceId, Subscriber, MemoryGroupId);
        }

    public:
        TActionGetFilter(const std::shared_ptr<IFilterSubscriber>& subscriber, const std::shared_ptr<NCommon::IDataSource>& source)
            : Subscriber(subscriber)
            , MemoryGroupId(source->GetMemoryGroupId()) {
        }
    };

private:
    TSourceIntervals Intervals;
    THashSet<ui64> FinishedSourceIds;
    TIntervalCounter NotFetchedSourcesCount;
    TIntervalsCursor NextIntervalToMerge;
    THashMap<ui64, std::shared_ptr<TSourceFilterConstructor>> ActiveSourceById;
    TRequestQueue RequestsBuffer;

    std::deque<std::shared_ptr<NSimple::IDataSource>> SortedSources;
    std::deque<std::shared_ptr<NSimple::IDataSource>> SkippedSources;
    std::deque<std::shared_ptr<TSourceFilterConstructor>> ActiveSources;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(TEvDuplicateFilterIntervalResult, Handle);
            hFunc(TEvDuplicateFilterDataFetched, Handle);
            hFunc(TEvNotifyReadingFinished, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterIntervalResult::TPtr&);
    void Handle(const TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const TEvNotifyReadingFinished::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);

    void AbortConstruction(const TString& reason);
    void StartFetchingColumns(const std::shared_ptr<TSourceFilterConstructor>& source, const ui64 memoryGroupId) const;
    void StartMergingColumns(const TIntervalsCursor& interval) const;
    void FlushFinishedSources();

    void HandleRequest(const ui64 sourceId, std::unique_ptr<IAction> action);
    void DoSkip(const ui64 sourceId);
    void DoGetFilter(const ui64 sourceId, const std::shared_ptr<IFilterSubscriber>& subscriber, const ui64 memoryGroupId);

    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceId(const ui64 sourceId) const;
    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceSeqNumber(const ui32 seqNumber) const;

public:
    TDuplicateFilterConstructor(const std::deque<std::shared_ptr<NSimple::IDataSource>>& sources);
};

}   // namespace NKikimr::NOlap::NReader
