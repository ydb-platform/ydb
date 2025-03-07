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

private:
    void Inc(const ui32 l, const ui32 r);

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);

    std::vector<ui32> DecAndGetZeros(const ui32 l, const ui32 r);
    bool IsAllZeros() const;
};

class TDuplicateFilterConstructor: public NActors::TActorBootstrapped<TDuplicateFilterConstructor> {
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
        using TRangeByPortionId = THashMap<ui64, TIntervalsRange>;
        YDB_READONLY_DEF(TRangeByPortionId, SourceRanges);
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

        TIntervalsRange GetRangeVerified(const ui64 portionId) const {
            return *TValidator::CheckNotNull(SourceRanges.FindPtr(portionId));
        }
    };

    class TSourceFilterConstructor {
    private:
        ui32 FirstIntervalIdx;
        YDB_READONLY_DEF(std::vector<std::optional<NArrow::TColumnFilter>>, IntervalFilters);
        YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
        std::shared_ptr<IFilterSubscriber> Subscriber;
        std::vector<ui64> RecordsInIntervals;
        ui64 ReadyFilterCount = 0;

    public:
        TSourceFilterConstructor(const std::shared_ptr<NCommon::IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& subscriber,
            const TSourceIntervals& intervals)
            : FirstIntervalIdx(intervals.GetRangeVerified(source->GetSourceIdx()).GetFirstIdx())
            , IntervalFilters(intervals.GetRangeVerified(source->GetSourceIdx()).NumIntervals())
            , Source(source)
            , Subscriber(subscriber) {
            // Not implemented (make records in intervals)
            RecordsInIntervals = {};
        }

        void SetFilter(const ui32 intervalIdx, NArrow::TColumnFilter&& filter);

        bool IsReady() const {
            return ReadyFilterCount == IntervalFilters.size();
        }

        void Finish() &&;
    };

    class TEvDuplicateFilterPartialResult
        : public NActors::TEventLocal<TEvDuplicateFilterPartialResult, NColumnShard::TEvPrivate::EvDuplicateFilterPartialResult> {
    private:
        YDB_READONLY(TConclusion<NArrow::TColumnFilter>, Result, NArrow::TColumnFilter::BuildAllowFilter());
        YDB_READONLY_DEF(ui32, IntervalIdx);
        YDB_READONLY_DEF(ui64, PortionId);

    public:
        TEvDuplicateFilterPartialResult(TConclusion<NArrow::TColumnFilter>&& result, const ui32 intervalIdx, const ui64 portionId)
            : Result(std::move(result))
            , IntervalIdx(intervalIdx)
            , PortionId(portionId) {
        }

        TConclusion<NArrow::TColumnFilter>&& ExtractResult() {
            return std::move(Result);
        }
    };

    class TInternalFilterSubscriber: public IFilterSubscriber {
    private:
        ui32 IntervalIdx;
        ui32 PortionId;
        TActorId Owner;

        virtual void OnFilterReady(const NArrow::TColumnFilter& result) override {
            TActorContext::AsActorContext().Send(
                Owner, new TDuplicateFilterConstructor::TEvDuplicateFilterPartialResult(result, IntervalIdx, PortionId));
        }

        virtual void OnFailure(const TString& reason) override {
            TActorContext::AsActorContext().Send(Owner,
                new TDuplicateFilterConstructor::TEvDuplicateFilterPartialResult(TConclusionStatus::Fail(reason), IntervalIdx, PortionId));
        }

    public:
        TInternalFilterSubscriber(const ui32 intervalIdx, const ui64 portionId, const TActorId& owner)
            : IntervalIdx(intervalIdx)
            , PortionId(portionId)
            , Owner(owner) {
        }
    };

private:
    TSourceIntervals Intervals;
    THashMap<ui64, TSourceFilterConstructor> AvailableSources;
    TRangeIndex AvailableSourcesCount;
    TIntervalCounter AwaitedSourcesCount;

public:
    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvDuplicateFilterPartialResult::TPtr&);

    TDuplicateFilterConstructor(const std::vector<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader
