#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scheduler.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader {

class TEvDuplicateFilterPartialResult
    : public NActors::TEventLocal<TEvDuplicateFilterPartialResult, NColumnShard::TEvPrivate::EvDuplicateFilterPartialResult> {
private:
    using TFilterBySourceId = THashMap<ui64, NArrow::TColumnFilter>;
    YDB_READONLY(TConclusion<TFilterBySourceId>, Result, TFilterBySourceId());
    YDB_READONLY_DEF(ui32, IntervalIdx);

public:
    TEvDuplicateFilterPartialResult(TConclusion<TFilterBySourceId>&& result, const ui32 intervalIdx)
        : Result(std::move(result))
        , IntervalIdx(intervalIdx) {
    }

    TFilterBySourceId&& DetachResult() {
        return Result.DetachResult();
    }
};

class TRangeIndex {
private:
    // TODO: optimize implementation
    THashMap<ui64, std::pair<ui32, ui32>> Intervals;

public:
    void AddRange(const ui32 l, const ui32 r, const ui64 id);
    void RemoveRange(const ui64 id);

    std::vector<ui64> FindIntersections(const ui64 p) const;
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

    class TZerosCollector {
    private:
        std::vector<ui32> FormerOnes;

    public:
        void OnUpdate(const TPosition& node, const ui64 newValue, const i64 delta) {
            AFL_VERIFY(delta == -1);
            if (newValue == 0) {
                for (ui32 i = node.GetLeft(); i <= node.GetRight(); ++i) {
                    FormerOnes.emplace_back(i);
                }
            }
        }

        std::vector<ui32> ExtractValues() {
            return std::move(FormerOnes);
        }
    };

    // Segment tree: Count[i] = GetCount(i * 2 + 1) + GetCount(i * 2 + 2)
    std::vector<ui64> Count;
    std::vector<i64> PropagatedDeltas;
    ui32 MaxIndex = 0;

private:
    void PropagateDelta(const TPosition& node);
    void Update(const TPosition& node, const TModification& modification, TZerosCollector* callback);
    void Inc(const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node) const {
        AFL_VERIFY(Count.size() == PropagatedDeltas.size());
        AFL_VERIFY(node.GetIndex() < Count.size());
        AFL_VERIFY(PropagatedDeltas[node.GetIndex()] * node.IntervalSize() >= (i64)Count[node.GetIndex()]);
        return Count[node.GetIndex()] + PropagatedDeltas[node.GetIndex()] * node.IntervalSize();
    }
    TPosition GetRoot() const {
        return TPosition(MaxIndex);
    }

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);
    std::vector<ui32> DecAndGetZeros(const ui32 l, const ui32 r);
    bool IsAllZeros() const;
};

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
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
        std::vector<NArrow::TReplaceKey> IntervalBorders;

    public:
        TSourceIntervals(const std::vector<std::shared_ptr<TPortionInfo>>& portions);

        const NArrow::TReplaceKey& GetRightInclusiveBorder(const ui32 intervalIdx) const {
            return IntervalBorders[intervalIdx];
        }
        const std::optional<NArrow::TReplaceKey> GetLeftExlusiveBorder(const ui32 intervalIdx) const {
            if (intervalIdx == 0) {
                return std::nullopt;
            }
            return IntervalBorders[intervalIdx - 1];
        }

        TIntervalsRange GetRangeVerified(const ui64 sourceId) const {
            const TIntervalsRange* findRange = SourceRanges.FindPtr(sourceId);
            AFL_VERIFY(findRange)("source", sourceId);
            return *findRange;
        }
    };

    class TSourceFilterConstructor {
    private:
        ui32 FirstIntervalIdx;
        YDB_READONLY_DEF(std::vector<std::optional<NArrow::TColumnFilter>>, IntervalFilters);
        YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
        std::shared_ptr<IFilterSubscriber> Subscriber;
        std::vector<ui64> IntervalOffsets;
        ui64 ReadyFilterCount = 0;
        std::optional<NSimple::ISourceFetchingScheduler::TSourceBlockedGuard> BlockGuard;

    public:
        TSourceFilterConstructor(const std::shared_ptr<NCommon::IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& subscriber,
            const TSourceIntervals& intervals)
            : FirstIntervalIdx(intervals.GetRangeVerified(source->GetSourceId()).GetFirstIdx())
            , IntervalFilters(intervals.GetRangeVerified(source->GetSourceId()).NumIntervals())
            , Source(source)
            , Subscriber(subscriber) {
            IntervalOffsets.emplace_back(0);
            const TIntervalsRange& sourceIntervals = intervals.GetRangeVerified(source->GetSourceId());
            for (ui32 intervalIdx = sourceIntervals.GetFirstIdx() + 1; intervalIdx <= sourceIntervals.GetLastIdx(); ++intervalIdx) {
                const NArrow::NAccessor::IChunkedArray::TRowRange findLeftBorder = source->GetStageData().ToGeneralContainer()->EqualRange(
                    *intervals.GetLeftExlusiveBorder(intervalIdx)
                         ->ToBatch(source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPrimaryKey()));
                IntervalOffsets.emplace_back(findLeftBorder.GetEnd());
            }
            AFL_VERIFY(IntervalOffsets.size() == sourceIntervals.NumIntervals());
        }

        void SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter);

        bool IsReady() const {
            return ReadyFilterCount == IntervalFilters.size();
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

        void SetBlockGuard(NSimple::ISourceFetchingScheduler::TSourceBlockedGuard&& guard) {
            AFL_VERIFY(!BlockGuard);
            BlockGuard.emplace(std::move(guard));
        }

        void Finish() &&;
        void AbortConstruction(const TString& reason) &&;
    };

private:
    TSourceIntervals Intervals;
    THashMap<ui64, TSourceFilterConstructor> AvailableSources;
    TRangeIndex AvailableSourcesCount;
    TIntervalCounter AwaitedSourcesCount;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(TEvDuplicateFilterPartialResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterPartialResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);

    void AbortConstruction(const TString& reason);

public:
    TDuplicateFilterConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader
