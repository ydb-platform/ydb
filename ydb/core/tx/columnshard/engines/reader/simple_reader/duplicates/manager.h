#pragma once

#include "common.h"
#include "context.h"
#include "events.h"
#include "private_events.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
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
    inline static const ui64 FILTER_CACHE_SIZE_CNT = 100;

    inline static TAtomicCounter NextRequestId = 0;

    const std::shared_ptr<NCommon::TColumnsSet> PKColumns;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    const TPortionIntervalTree Intervals;
    const THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter> FiltersCache;
    THashMap<TDuplicateMapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>> BuildingFilters;
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    const std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;

private:
    static TPortionIntervalTree MakeIntervalTree(const std::deque<NSimple::TSourceConstructor>& portions) {
        TPortionIntervalTree intervals;
        for (const auto& portion : portions) {
            intervals.AddRange(TPortionIntervalTree::TOwnedRange(portion.GetPortion()->IndexKeyStart(), true,
                                   portion.GetPortion()->IndexKeyEnd(), true), portion.GetPortion());
        }
        return intervals;
    }

    static THashMap<ui64, std::shared_ptr<TPortionInfo>> MakePortionsIndex(const TPortionIntervalTree& intervals) {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
        intervals.EachRange(
            [&portions](const TPortionIntervalTree::TOwnedRange& /*range*/, const std::shared_ptr<TPortionInfo>& portion) mutable {
                AFL_VERIFY(portions.emplace(portion->GetPortionId(), portion).second);
            });
        return portions;
    }

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NPrivate::TEvDuplicateSourceCacheResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        AbortAndPassAway("aborted by actor system");
    }

    void AbortAndPassAway(const TString& reason) {
        for (auto& [_, constructors] : BuildingFilters) {
            for (auto& constructor : constructors) {
                if (!constructor->IsDone()) {
                    constructor->Abort(reason);
                }
            }
        }
        PassAway();
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        const auto* portion = Portions.FindPtr(portionId);
        AFL_VERIFY(portion)("portion", portionId);
        return *portion;
    }

    ui64 MakeRequestId() {
        return NextRequestId.Inc();
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

public:
    TDuplicateManager(const TSpecialReadContext& context, const std::deque<NSimple::TSourceConstructor>& portions);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
