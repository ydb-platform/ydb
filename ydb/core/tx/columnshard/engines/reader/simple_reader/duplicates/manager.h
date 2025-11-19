#pragma once

#include "common.h"
#include "context.h"
#include "events.h"
#include "private_events.h"
#include "splitter.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/constructors.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/range_treap/range_treap.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;
}   // namespace NKikimr::NOlap::NReader::NSimple

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TDuplicateManager: public NActors::TActor<TDuplicateManager> {
    friend class TMergeableInterval;

private:
    class TFilterSizeProvider {
    public:
        size_t operator()(const NArrow::TColumnFilter& filter) {
            return filter.GetDataSize();
        }
    };

    class TIntervalFilterCallback {
    private:
        ui32 IntervalIdx;
        std::shared_ptr<TFilterAccumulator> Constructor;

    public:
        TIntervalFilterCallback(const ui32 intervalIdx, const std::shared_ptr<TFilterAccumulator>& constructor)
            : IntervalIdx(intervalIdx)
            , Constructor(constructor)
        {
        }

        void OnFilterReady(const NArrow::TColumnFilter& filter) {
            Constructor->AddFilter(IntervalIdx, filter);
        }

        void OnError(const TString& error) {
            Constructor->Abort(error);
        }
    };

    class TIntervalInFlightInfo {
    private:
        THashMap<ui64, std::vector<TIntervalFilterCallback>> SubscribersByPortion;
        std::shared_ptr<TJobStatus> Job;

    public:
        TIntervalInFlightInfo() = default;

        void SetJob(const std::shared_ptr<TJobStatus>& job) {
            AFL_VERIFY(!Job);
            AFL_VERIFY(job);
            Job = job;
        }

        void AddSubscriber(ui64 portionId, TIntervalFilterCallback&& callback) {
            AFL_VERIFY(SubscribersByPortion.emplace(portionId, std::vector<TIntervalFilterCallback>({std::move(callback)})).second);
        }

        bool OnFilterReady(const ui64 portionId, const NArrow::TColumnFilter& filter) {
            if (auto findPortion = SubscribersByPortion.find(portionId); findPortion != SubscribersByPortion.end()) {
                for (auto&& subscriber : findPortion->second) {
                    subscriber.OnFilterReady(filter);
                }
                SubscribersByPortion.erase(findPortion);
                return true;
            }
            return false;
        }
        void OnError(const TString& error) {
            for (auto&& [_, subscribers] : SubscribersByPortion) {
                for (auto&& subscriber : subscribers) {
                    subscriber.OnError(error);
                }
            }
            SubscribersByPortion.clear();
        }

        bool IsDone() const {
            return SubscribersByPortion.empty();
        }

        void ValidateProgress() const {
            if (!SubscribersByPortion.empty()) {
                AFL_VERIFY(Job);
                AFL_VERIFY(!Job->IsDone());
            }
        }
    };

private:
    inline static const ui64 FILTER_CACHE_SIZE = 10000000;  // 10 MiB
    inline static const ui64 BORDER_CACHE_SIZE_COUNT = 10000;

    const std::shared_ptr<ISnapshotSchema> LastSchema;
    const std::shared_ptr<NCommon::TColumnsSet> PKColumns;
    const std::shared_ptr<arrow::Schema> PKSchema;
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    const TPortionIntervalTree Intervals;
    const std::shared_ptr<TPortionStore> Portions;
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    const std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;

    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter, TNoopDelete, TFilterSizeProvider> FiltersCache;
    TLRUCache<ui64, TSortableBorders> MaterializedBordersCache;
    THashMap<TIntervalBordersView, TIntervalInFlightInfo> IntervalsInFlight;
    ui64 ExpectedIntersectionCount = 0;
    std::shared_ptr<TAtomicCounter> AbortionFlag;

private:
    static TPortionIntervalTree MakeIntervalTree(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
        TPortionIntervalTree intervals;
        for (const auto& portion : portions) {
            intervals.AddRange(TPortionIntervalTree::TOwnedRange(portion->IndexKeyStart(), true,
                                   portion->IndexKeyEnd(), true), portion);
        }
        return intervals;
    }

    static std::shared_ptr<TPortionStore> MakePortionsIndex(const TPortionIntervalTree& intervals) {
        THashMap<ui64, TPortionInfo::TConstPtr> portions;
        intervals.EachRange(
            [&portions](const TPortionIntervalTree::TOwnedRange& /*range*/, const std::shared_ptr<TPortionInfo>& portion) mutable {
                AFL_VERIFY(portions.emplace(portion->GetPortionId(), portion).second);
                return true;
            });
        return std::make_shared<TPortionStore>(std::move(portions));
    }

    bool IsExclusiveInterval(const NArrow::TSimpleRow& begin, const NArrow::TSimpleRow& end) const;
    void ValidateInFlightProgress() const {
        for (const auto& [_, inFlight] : IntervalsInFlight) {
            inFlight.ValidateProgress();
        }
    }

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterRequestResourcesAllocated, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr&);
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        AbortAndPassAway("aborted by actor system");
    }

    void AbortAndPassAway(const TString& reason) {
        AbortionFlag->Inc();
        for (auto& [_, info] : IntervalsInFlight) {
            info.OnError(reason);
        }
        PassAway();
    }

    std::map<ui32, std::shared_ptr<arrow::Field>> GetFetchingColumns() const {
        std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
        {
            for (const auto& columnId : PKColumns->GetColumnIds()) {
                fieldsByColumn.emplace(columnId, PKColumns->GetFilteredSchemaVerified().GetFieldByColumnIdVerified(columnId));
            }
            for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
                fieldsByColumn.emplace(columnId, IIndexInfo::GetColumnFieldVerified(columnId));
            }
        }
        return fieldsByColumn;
    }

    TSortableBorders GetBorders(const ui64 portionId) {
        auto findCached = MaterializedBordersCache.Find(portionId);
        if (findCached != MaterializedBordersCache.End()) {
            return findCached.Value();
        }
        const auto& portion = Portions->GetPortionVerified(portionId);
        TSortableBorders result =
            TSortableBorders(std::make_shared<NArrow::NMerger::TSortableBatchPosition>(portion->IndexKeyStart().BuildSortablePosition()),
                std::make_shared<NArrow::NMerger::TSortableBatchPosition>(portion->IndexKeyEnd().BuildSortablePosition()));
        MaterializedBordersCache.Insert(portionId, result);
        return result;
    }

    TIntervalsIterator StartIntervalProcessing(
        const THashSet<ui64>& intersectingPortions, const std::shared_ptr<TFilterAccumulator>& constructor);

public:
    TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
