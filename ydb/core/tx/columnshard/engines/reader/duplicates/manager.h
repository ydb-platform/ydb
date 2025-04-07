#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader {

namespace NSimple {
class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
}

class TColumnFetchingContext;

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

class TEvDuplicateFilterStartFetching
    : public NActors::TEventLocal<TEvDuplicateFilterStartFetching, NColumnShard::TEvPrivate::EvDuplicateFilterStartFetching> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TColumnFetchingContext>, Context);
    YDB_READONLY_DEF(std::shared_ptr<NBlobOperations::NRead::ITask>, NextAction);

public:
    TEvDuplicateFilterStartFetching(
        const std::shared_ptr<TColumnFetchingContext>& context, const std::shared_ptr<NBlobOperations::NRead::ITask>& nextAction)
        : Context(context)
        , NextAction(nextAction) {
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

    class TZeroCollector {
    private:
        std::vector<ui32> Positions;

    public:
        void OnNewZeros(const TPosition& node) {
            for (ui32 i = node.GetLeft(); i <= node.GetRight(); ++i) {
                Positions.emplace_back(i);
            }
        }

        std::vector<ui32> ExtractNewZeroPositions() {
            return std::move(Positions);
        }
    };

    // Segment tree: Count[i] = GetCount(i * 2 + 1) + GetCount(i * 2 + 2)
    std::vector<ui64> Count;
    std::vector<ui64> MinValue;
    std::vector<i64> PropagatedDeltas;
    ui32 MaxIndex = 0;

private:
    void PropagateDelta(const TPosition& node, TZeroCollector* callback);
    void Update(const TPosition& node, const TModification& modification, TZeroCollector* callback);
    void Inc(const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node, const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node) const {
        AFL_VERIFY(node.GetIndex() < Count.size());
        return Count[node.GetIndex()] +
               (node.GetIndex() < PropagatedDeltas.size() ? PropagatedDeltas[node.GetIndex()] : 0) * node.IntervalSize();
    }
    ui64 GetMinValue(const TPosition& node) const {
        AFL_VERIFY(node.GetIndex() < Count.size());
        return MinValue[node.GetIndex()] + (node.GetIndex() < PropagatedDeltas.size() ? PropagatedDeltas[node.GetIndex()] : 0);
    }
    TPosition GetRoot() const {
        return TPosition(MaxIndex);
    }

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);
    std::vector<ui32> DecAndGetZeros(const ui32 l, const ui32 r);
    ui64 GetCount(const ui32 l, const ui32 r) {
        return GetCount(GetRoot(), l, r);
    }
    bool IsAllZeros() const;
};

class TIntervalSet {
private:
    using TPosition = ui32;
    using TIntervalId = ui64;

private:
    class TBorder {
    private:
        YDB_READONLY_DEF(bool, IsEnd);
        YDB_READONLY_DEF(TPosition, Position);
        YDB_READONLY_DEF(TIntervalId, Interval);

        TBorder(const TPosition position, const TIntervalId interval, const bool isEnd)
            : IsEnd(isEnd)
            , Position(position)
            , Interval(interval) {
        }

    public:
        static TBorder Begin(const TPosition position, const TIntervalId interval) {
            return TBorder(position, interval, false);
        }
        static TBorder End(const TPosition position, const TIntervalId interval) {
            return TBorder(position, interval, true);
        }

        std::partial_ordering operator<=>(const TBorder& other) const {
            return std::tie(IsEnd, Position, Interval) <=> std::tie(other.IsEnd, Position, other.Interval);
        }
    };

    std::set<TBorder> Borders;

public:
    void Insert(const TPosition l, const TPosition r, const TIntervalId id) {
        AFL_VERIFY(Borders.insert(TBorder::Begin(l, id)).second);
        AFL_VERIFY(Borders.insert(TBorder::End(r, id)).second);
    }

    void Erase(const TPosition l, const TPosition r, const TIntervalId id) {
        AFL_VERIFY(Borders.erase(TBorder::Begin(l, id)));
        AFL_VERIFY(Borders.erase(TBorder::End(r, id)));
    }

    THashSet<ui64> GetIntersections(const ui32 l, const ui32 r) const {
        // NOTE: fully overlaying intervals are not counted: [L, R] where L < l < r < R
        THashSet<ui64> intervals;
        for (auto it = Borders.lower_bound(TBorder::Begin(l, std::numeric_limits<TIntervalId>::min()));
             it != Borders.end() && *it <= TBorder::End(r, std::numeric_limits<TIntervalId>::max()); ++it) {
            intervals.insert(it->GetInterval());
        }
        return intervals;
    }
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

        TString DebugString() const {
            return TStringBuilder() << '[' << FirstIdx << ',' << LastIdx << ']';
        }
    };

    class TSourceIntervals {
    private:
        using TRangeBySourceId = THashMap<ui64, TIntervalsRange>;
        YDB_READONLY_DEF(TRangeBySourceId, SourceRanges);
        YDB_READONLY_DEF(std::vector<ui64>, SortedSourceIds);
        std::vector<NArrow::TReplaceKey> IntervalBorders;

    public:
        TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& sources);

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

    class TSourceFilterConstructor: NColumnShard::TMonitoringObjectsCounter<TSourceFilterConstructor> {
    private:
        ui32 FirstIntervalIdx;
        YDB_READONLY_DEF(std::vector<std::optional<NArrow::TColumnFilter>>, IntervalFilters);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, ColumnData);
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> MemoryGuard;
        YDB_READONLY_DEF(std::shared_ptr<NSimple::TPortionDataSource>, Source);
        std::shared_ptr<IFilterSubscriber> Subscriber;
        std::vector<ui64> IntervalOffsets;
        std::vector<NArrow::TReplaceKey> RightIntervalBorders;
        ui64 ReadyFilterCount = 0;

    public:
        TSourceFilterConstructor(const std::shared_ptr<NSimple::TPortionDataSource>& source, const TSourceIntervals& intervals);

        void SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter);
        void SetMemoryGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
            AFL_VERIFY(!MemoryGuard);
            MemoryGuard = std::move(guard);
        }
        void SetColumnData(std::shared_ptr<NArrow::TGeneralContainer>&& data);
        void SetSubscriber(const std::shared_ptr<IFilterSubscriber>& subscriber) {
            AFL_VERIFY(!Subscriber);
            Subscriber = subscriber;
        }

        bool IsReady() const {
            return Subscriber && ReadyFilterCount == IntervalFilters.size();
        }

        NArrow::NAccessor::IChunkedArray::TRowRange GetIntervalRange(const ui32 globalIntervalIdx) const;

        void Finish();
        void AbortConstruction(const TString& reason);
    };

    class TWaitingSourceInfo {
    private:
        std::atomic_uint64_t FetchingMemoryGroupId = std::numeric_limits<uint64_t>::max();
        std::atomic_bool IsFetchingStarted = false;
        ui32 PortionIdx;

    public:
        TWaitingSourceInfo(const ui32 portionIdx)
            : PortionIdx(portionIdx) {
        }

        [[nodiscard]] bool SetStartAllocation(const ui64 memoryGroupId) {
            ui64 oldMemGroup = FetchingMemoryGroupId.load();
            while (true) {
                if (oldMemGroup < memoryGroupId) {
                    return false;
                }
                if (FetchingMemoryGroupId.compare_exchange_weak(oldMemGroup, memoryGroupId)) {
                    break;
                }
            }

            AFL_VERIFY(oldMemGroup != memoryGroupId);
            if (IsFetchingStarted.load()) {
                return false;
            }
            return true;
        }

        bool SetStartFetching() {
            return !IsFetchingStarted.exchange(true);
        }

        std::shared_ptr<NSimple::TPortionDataSource> Construct(const std::shared_ptr<NSimple::TSpecialReadContext>& context) const;
    };

private:
    THashMap<ui64, std::shared_ptr<TWaitingSourceInfo>> WaitingSources;
    THashMap<ui64, std::shared_ptr<TSourceFilterConstructor>> ActiveSources;

    const TSourceIntervals Intervals;
    TIntervalCounter NotFetchedSourcesCount;
    TIntervalSet NotFetchedSourcesIndex;
    THashMap<ui32, std::vector<ui64>> FetchedSourcesByInterval;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(TEvDuplicateFilterIntervalResult, Handle);
            hFunc(TEvDuplicateFilterDataFetched, Handle);
            hFunc(TEvDuplicateFilterStartFetching, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterStartFetching::TPtr&);
    void Handle(const TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const TEvDuplicateFilterIntervalResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);

    void AbortAndPassAway(const TString& reason);
    void StartAllocation(const ui64 sourceId, const std::shared_ptr<NSimple::IDataSource>& requester);
    void StartMergingColumns(const ui32 intervalIdx);

    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceId(const ui64 sourceId) const;
    std::shared_ptr<TSourceFilterConstructor> GetConstructorBySourceSeqNumber(const ui32 seqNumber) const;

public:
    TDuplicateFilterConstructor(const NSimple::TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader
