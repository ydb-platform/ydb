#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader {

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
    // NOTE: This is a placeholder implementation: unoptimized
    // TODO: O(log(N)) updates, O(log(N)) queries (option: segment tree)
    THashMap<ui32, ui32> Count;
    // Segment tree: Count[i] has nodes Count[i * 2 + 1], Count[i * 2 + 2]
    // std::vector<ui64> Count;
    // std::vector<i64> PropagateDelta;

private:
    std::vector<ui32> UpdateAndGetFormerOnes(const ui32 l, const ui32 r, const ui32 lSeg, const ui32 rSeg);
    void Inc(const ui32 l, const ui32 r);

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

        void Finish() &&;
        void AbortConstruction(const TString& reason) &&;
    };

    class TEvDuplicateFilterPartialResult
        : public NActors::TEventLocal<TEvDuplicateFilterPartialResult, NColumnShard::TEvPrivate::EvDuplicateFilterPartialResult> {
    private:
        YDB_READONLY(TConclusion<NArrow::TColumnFilter>, Result, NArrow::TColumnFilter::BuildAllowFilter());
        YDB_READONLY_DEF(ui32, IntervalIdx);
        YDB_READONLY_DEF(ui64, SourceId);

    public:
        TEvDuplicateFilterPartialResult(TConclusion<NArrow::TColumnFilter>&& result, const ui32 intervalIdx, const ui64 sourceId)
            : Result(std::move(result))
            , IntervalIdx(intervalIdx)
            , SourceId(sourceId) {
        }

        TConclusion<NArrow::TColumnFilter>&& ExtractResult() {
            return std::move(Result);
        }
    };

    class TInternalFilterSubscriber: public IFilterSubscriber {
    private:
        ui32 IntervalIdx;
        ui32 SourceId;
        TActorId Owner;

        virtual void OnFilterReady(const NArrow::TColumnFilter& result) override {
            TActorContext::AsActorContext().Send(
                Owner, new TDuplicateFilterConstructor::TEvDuplicateFilterPartialResult(result, IntervalIdx, SourceId));
        }

        virtual void OnFailure(const TString& reason) override {
            TActorContext::AsActorContext().Send(
                Owner, new TDuplicateFilterConstructor::TEvDuplicateFilterPartialResult(TConclusionStatus::Fail(reason), IntervalIdx, SourceId));
        }

    public:
        TInternalFilterSubscriber(const ui32 intervalIdx, const ui64 sourceId, const TActorId& owner)
            : IntervalIdx(intervalIdx)
            , SourceId(sourceId)
            , Owner(owner) {
        }
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
