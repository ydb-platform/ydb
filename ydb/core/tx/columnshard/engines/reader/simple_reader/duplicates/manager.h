#pragma once

#include "common.h"
#include "context.h"
#include "events.h"
#include "filters.h"
#include "private_events.h"

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
    const std::shared_ptr<ISnapshotSchema> LastSchema;
    const std::shared_ptr<NCommon::TColumnsSet> PKColumns;
    const std::shared_ptr<arrow::Schema> PKSchema;
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
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

    std::unordered_map<ui64, NArrow::TColumnFilter> ReadyFilters; // PortionId -> TColumnFilter
    std::shared_ptr<TAtomicCounter> AbortionFlag;
    ui64 PrevRowsAdded = 0;
    ui64 PrevRowsSkipped = 0;
    std::unordered_set<ui64> ExclusivePortions;

private:
    static NArrow::NMerger::TCursor GetVersionBatch(const TSnapshot& snapshot, const ui64 writeId) {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, writeId);
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    };

    static std::shared_ptr<TPortionStore> MakePortionsIndex(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
        THashMap<ui64, TPortionInfo::TConstPtr> portionsStore;
        for (const auto& portion: portions) {
            AFL_VERIFY(portionsStore.emplace(portion->GetPortionId(), portion).second);
        }
        return std::make_shared<TPortionStore>(std::move(portionsStore));
    }

    bool IsExclusiveInterval(const ui64 portionId) const;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterRequestResourcesAllocated, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(TEvIntervalConstructionResult, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr&);
    void Handle(const TEvIntervalConstructionResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        Counters->OnLeftBorders(-1 * static_cast<i64>(Borders.size()));
        Counters->OnWaitingBorders(-1 * static_cast<i64>(WaitingBorders.size()));
        AbortAndPassAway("aborted by actor system");
    }

    void AbortAndPassAway(const TString& error) {
        AbortionFlag->Inc();
        FiltersBuilder.Abort(error);
        PassAway();
    }

    std::map<ui32, std::shared_ptr<arrow::Field>> GetFetchingColumns() const {
        std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
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

public:
    TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
