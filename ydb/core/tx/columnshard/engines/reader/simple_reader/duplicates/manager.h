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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
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
<<<<<<< HEAD

=======
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
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
<<<<<<< HEAD
    inline static const ui64 FILTER_CACHE_SIZE = 10000000;   // 10 MiB
    inline static const ui64 BORDER_CACHE_SIZE_COUNT = 10000;

=======
>>>>>>> 40c8babe329 (Deduplication based on merge (#36186))
=======
    inline static const ui64 FILTER_CACHE_SIZE = 10000000;  // 10 MiB
    inline static const ui64 BORDER_CACHE_SIZE_COUNT = 10000;

>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
    const std::shared_ptr<ISnapshotSchema> LastSchema;
    const std::shared_ptr<NCommon::TColumnsSet> PKColumns;
    const std::shared_ptr<arrow::Schema> PKSchema;
    const std::shared_ptr<NColumnShard::TSimpleDuplicateFilteringCounters> Counters;
    const TPortionIntervalTree Intervals;
    const std::shared_ptr<TPortionStore> Portions;
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    const std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;
    NArrow::NMerger::TMergePartialStream Merger;
    TFiltersBuilder FiltersBuilder;
    
    struct TBorderInfo {
        std::vector<ui64> Start;
        std::vector<ui64> Finish;
    };
    std::map<NArrow::TSimpleRow, TBorderInfo> Borders;
    THashSet<ui64> CurrentPortions;
    THashSet<ui64> ProcessedPortions;
    std::map<NArrow::TSimpleRow, ui32> WaitingBorders;
    std::optional<NArrow::TSimpleRow> PreviousBorder;
    
    std::optional<NArrow::TSimpleRow> LastBorder;

<<<<<<< HEAD
    std::unordered_map<ui64, NArrow::TColumnFilter> ReadyFilters; // PortionId -> TColumnFilter
=======
    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter, TNoopDelete, TFilterSizeProvider> FiltersCache;
    TLRUCache<ui64, TSortableBorders> MaterializedBordersCache;
    THashMap<TIntervalBordersView, TIntervalInFlightInfo> IntervalsInFlight;
    ui64 ExpectedIntersectionCount = 0;
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
    std::shared_ptr<TAtomicCounter> AbortionFlag;
    ui64 PrevRowsAdded = 0;
    ui64 PrevRowsSkipped = 0;
    std::unordered_set<ui64> ExclusivePortions;

private:
<<<<<<< HEAD
<<<<<<< HEAD
    static TPortionIntervalTree MakeIntervalTree(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
        TPortionIntervalTree intervals;
        for (const auto& portion : portions) {
            intervals.AddRange(TPortionIntervalTree::TOwnedRange(portion->IndexKeyStart(), true, portion->IndexKeyEnd(), true), portion);
=======
    static NArrow::NMerger::TCursor GetVersionBatch(const TSnapshot& snapshot, const ui64 writeId) {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, writeId);
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    };

    static std::shared_ptr<TPortionStore> MakePortionsIndex(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
        THashMap<ui64, TPortionInfo::TConstPtr> portionsStore;
        for (const auto& portion: portions) {
            AFL_VERIFY(portionsStore.emplace(portion->GetPortionId(), portion).second);
>>>>>>> 40c8babe329 (Deduplication based on merge (#36186))
        }
        return std::make_shared<TPortionStore>(std::move(portionsStore));
    }

<<<<<<< HEAD
=======
    static TPortionIntervalTree MakeIntervalTree(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
        TPortionIntervalTree intervals;
        for (const auto& portion : portions) {
            intervals.AddRange(TPortionIntervalTree::TOwnedRange(portion->IndexKeyStart(), true,
                                   portion->IndexKeyEnd(), true), portion);
        }
        return intervals;
    }

>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
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
<<<<<<< HEAD

=======
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
    void ValidateInFlightProgress() const {
        for (const auto& [_, inFlight] : IntervalsInFlight) {
            inFlight.ValidateProgress();
        }
    }
<<<<<<< HEAD
=======
    bool IsExclusiveInterval(const ui64 portionId) const;
>>>>>>> 40c8babe329 (Deduplication based on merge (#36186))
=======
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterRequestResourcesAllocated, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
<<<<<<< HEAD
            hFunc(TEvIntervalConstructionResult, Handle);
=======
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr&);
<<<<<<< HEAD
<<<<<<< HEAD
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);

=======
    void Handle(const TEvIntervalConstructionResult::TPtr&);
>>>>>>> 40c8babe329 (Deduplication based on merge (#36186))
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        Counters->OnLeftBorders(-1 * static_cast<i64>(Borders.size()));
        Counters->OnWaitingBorders(-1 * static_cast<i64>(WaitingBorders.size()));
        AbortAndPassAway("aborted by actor system");
    }

    void AbortAndPassAway(const TString& error) {
        AbortionFlag->Inc();
        FiltersBuilder.Abort(error);
=======
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        AbortAndPassAway("aborted by actor system");
    }

    void AbortAndPassAway(const TString& reason) {
        AbortionFlag->Inc();
        for (auto& [_, info] : IntervalsInFlight) {
            info.OnError(reason);
        }
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
        PassAway();
    }

    std::map<ui32, std::shared_ptr<arrow::Field>> GetFetchingColumns() const {
        std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
<<<<<<< HEAD
        for (const auto& columnId : PKColumns->GetColumnIds()) {
            fieldsByColumn.emplace(columnId, PKColumns->GetFilteredSchemaVerified().GetFieldByColumnIdVerified(columnId));
        }
        for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
            fieldsByColumn.emplace(columnId, IIndexInfo::GetColumnFieldVerified(columnId));
        }
        return fieldsByColumn;
    }
    
private:
    void BuildExclusivePortions();
=======
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
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))

public:
    TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
