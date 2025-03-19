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

class TEvDuplicateFilterPartialResult
    : public NActors::TEventLocal<TEvDuplicateFilterPartialResult, NColumnShard::TEvPrivate::EvDuplicateFilterPartialResult> {
private:
    using TFilterBySourceIdx = THashMap<ui32, NArrow::TColumnFilter>;
    YDB_READONLY(TConclusion<TFilterBySourceIdx>, Result, TFilterBySourceIdx());
    YDB_READONLY_DEF(ui32, IntervalIdx);

public:
    TEvDuplicateFilterPartialResult(TConclusion<TFilterBySourceIdx>&& result, const ui32 intervalIdx)
        : Result(std::move(result))
        , IntervalIdx(intervalIdx) {
    }

    TFilterBySourceIdx&& DetachResult() {
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
        TPosition(const ui32 maxIndex)
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
    ui64 GetCount(const TPosition& node, const ui32 l, const ui32 r) const;
    ui64 GetCount(const TPosition& node) const {
        AFL_VERIFY(Count.size() == PropagatedDeltas.size());
        AFL_VERIFY(node.GetIndex() < Count.size());
        return Count[node.GetIndex()] + PropagatedDeltas[node.GetIndex()] * node.IntervalSize();
    }
    TPosition GetRoot() const {
        return TPosition(MaxIndex);
    }

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);
    void Dec(const ui32 l, const ui32 r);
    ui64 GetCount(const ui32 l, const ui32 r) const {
        return GetCount(GetRoot(), l, r);
    }
    bool IsAllZeros() const;
};

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
    friend class TColumnFetchingContext;

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

        // bool ContainsInterval(const ui32 intervalIdx) const {
        //     return FirstIdx <= intervalIdx && intervalIdx <= LastIdx;
        // }
    };

    class TSourceIntervals {
    private:
        using TRangeBySourceId = THashMap<ui64, TIntervalsRange>;
        YDB_READONLY_DEF(TRangeBySourceId, SourceRanges);
        std::vector<ui64> SortedSourceIds;
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

        TIntervalsRange GetRangeBySourceIndex(const ui32 sourceIdx) const {
            AFL_VERIFY(sourceIdx < SortedSourceIds.size());
            return GetRangeBySourceId(SortedSourceIds[sourceIdx]);
        }
    };

    class TIntervalsCursor {
    private:
        using TSourceIdxByIntervalIdx = std::map<ui32, ui32>;
        YDB_READONLY(ui32, IntervalIdx, 0);
        YDB_READONLY_DEF(TSourceIdxByIntervalIdx, SourcesByRightInterval);
        const TSourceIntervals* Owner;
        ui32 NextSourceIdx = 0;

        void NextImpl(const bool init = false) {
            if (init) {
                AFL_VERIFY(IntervalIdx == 0);
            } else {
                ++IntervalIdx;
            }

            while (!SourcesByRightInterval.empty() && SourcesByRightInterval.begin()->first < IntervalIdx) {
                SourcesByRightInterval.erase(SourcesByRightInterval.begin());
            }

            while (NextSourceIdx != Owner->NumSources() && Owner->GetRangeBySourceIndex(NextSourceIdx).GetFirstIdx() <= IntervalIdx) {
                SourcesByRightInterval.emplace(Owner->GetRangeBySourceIndex(NextSourceIdx).GetFirstIdx(), NextSourceIdx);
                ++NextSourceIdx;
            }

            AFL_VERIFY(IntervalIdx <= Owner->NumIntervals())("i", IntervalIdx)("num_intervals", Owner->NumIntervals());
            if (IntervalIdx == Owner->NumIntervals()) {
                AFL_VERIFY(SourcesByRightInterval.empty());
            }
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
        ui64 ReadyFilterCount = 0;

    public:
        TSourceFilterConstructor(const std::shared_ptr<NCommon::IDataSource>& source,
            const TSourceIntervals& intervals)
            : FirstIntervalIdx(intervals.GetRangeBySourceId(source->GetSourceId()).GetFirstIdx())
            , IntervalFilters(intervals.GetRangeBySourceId(source->GetSourceId()).NumIntervals())
            , Source(source) {
            IntervalOffsets.emplace_back(0);
            const TIntervalsRange& sourceIntervals = intervals.GetRangeBySourceId(source->GetSourceId());
            for (ui32 intervalIdx = sourceIntervals.GetFirstIdx() + 1; intervalIdx <= sourceIntervals.GetLastIdx(); ++intervalIdx) {
                const NArrow::NAccessor::IChunkedArray::TRowRange findLeftBorder = source->GetStageData().ToGeneralContainer()->EqualRange(
                    *intervals.GetLeftExlusiveBorder(intervalIdx)
                         ->ToBatch(source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey()));
                // TODO: handle reverse (now it's supposed to be false)
                AFL_VERIFY(!source->GetContext()->GetReadMetadata()->IsDescSorted());  // TODO remove
                IntervalOffsets.emplace_back(findLeftBorder.GetEnd());
            }
            AFL_VERIFY(IntervalOffsets.size() == sourceIntervals.NumIntervals());
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
        }
        void SetSubscriber(const std::shared_ptr<IFilterSubscriber>& subscriber) {
            AFL_VERIFY(!Subscriber);
            Subscriber = subscriber;
        }

        bool IsReady() const {
            return Subscriber && ReadyFilterCount == IntervalFilters.size();
        }

        NArrow::NAccessor::IChunkedArray::TRowRange GetIntervalRange(const ui32 globalIntervalIdx) const {
            AFL_VERIFY(globalIntervalIdx >= FirstIntervalIdx)("global", globalIntervalIdx)("first", FirstIntervalIdx);
            const ui32 localIntervalIdx = globalIntervalIdx - FirstIntervalIdx;
            AFL_VERIFY(localIntervalIdx < IntervalOffsets.size())("local", localIntervalIdx)("global", globalIntervalIdx)(
                                              "size", IntervalOffsets.size());
            if (localIntervalIdx == IntervalOffsets.size() - 1) {
                return { IntervalOffsets[localIntervalIdx], Source->GetStageData().GetTable()->GetRecordsCountVerified() };
            } else {
                return { IntervalOffsets[localIntervalIdx], IntervalOffsets[localIntervalIdx + 1] };
            }
        }

        void Finish();
        void AbortConstruction(const TString& reason);
    };

private:
    TSourceIntervals Intervals;
    ui64 FinishedSourcesCount = 0;
    std::deque<std::shared_ptr<TSourceFilterConstructor>> ActiveSources;
    std::deque<std::shared_ptr<NSimple::IDataSource>> SortedSources;
    TIntervalCounter NotFetchedSourcesCount;
    TIntervalsCursor NextIntervalToMerge;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(TEvDuplicateFilterPartialResult, Handle);
            hFunc(TEvDuplicateFilterDataFetched, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterPartialResult::TPtr&);
    void Handle(const TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);

    void AbortConstruction(const TString& reason);
    void StartFetchingColumns(const std::shared_ptr<TSourceFilterConstructor>& source, const ui64 memoryGroupId) const;
    void StartMergingColumns(const TIntervalsCursor& interval) const;
    void FlushFinishedSources() ;

    std::shared_ptr<TSourceFilterConstructor> GetConstructorVerified(const ui32 sourceIdx) const;

public:
    TDuplicateFilterConstructor(const std::deque<std::shared_ptr<NSimple::IDataSource>>& sources);
};

}   // namespace NKikimr::NOlap::NReader
