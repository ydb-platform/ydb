#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader {

class TRangeIndex {
private:
    // TODO: optimize implementation
    THashMap<ui32, std::pair<ui32, ui32>> Intervals;

public:
    void AddRange(const ui32 l, const ui32 r, const ui32 id);
    void RemoveRange(const ui32 id);

    std::vector<ui32> FindIntersections(const ui32 p) const;
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

class TDuplicateFilterConstructor: NActors::TActorBootstrapped<TDuplicateFilterConstructor> {
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
        using TRangeBySourceIdx = THashMap<ui32, TIntervalsRange>;
        YDB_READONLY_DEF(TRangeBySourceIdx, SourceRanges);
        std::vector<NArrow::TReplaceKey> IntervalBorders;

    public:
        TSourceIntervals(const std::vector<std::shared_ptr<NCommon::IDataSource>>& sources);

        const NArrow::TReplaceKey& GetRightInclusiveBorder(const ui32 intervalIdx) const {
            return IntervalBorders[intervalIdx];
        }
        const std::optional<NArrow::TReplaceKey> GetLeftExlusiveBorder(const ui32 intervalIdx) const {
            if (intervalIdx == 0) {
                return std::nullopt;
            }
            return IntervalBorders[intervalIdx - 1];
        }

        TIntervalsRange GetRangeVerified(const ui32 sourceIdx) const {
            return *TValidator::CheckNotNull(SourceRanges.FindPtr(sourceIdx));
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

private:
    TSourceIntervals Intervals;
    THashMap<ui32, TSourceFilterConstructor> AvailableSources;
    TRangeIndex AvailableSourcesCount;
    TIntervalCounter AwaitedSourcesCount;

public:
    void Handle(const TEvRequestFilter::TPtr&);

    TDuplicateFilterConstructor(const std::vector<std::shared_ptr<NCommon::IDataSource>>& sources);
};

}   // namespace NKikimr::NOlap::NReader
