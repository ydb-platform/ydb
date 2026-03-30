#pragma once

#include "borders_flow_controller.h"
#include "common.h"
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

    TBordersFlowController BordersFlowController;
    TFiltersStore FiltersStore;
    std::shared_ptr<TAtomicCounter> AbortionFlag;

private:
    static NArrow::NMerger::TCursor GetVersionBatch(const TSnapshot& snapshot, const ui64 writeId);
    static std::shared_ptr<TPortionStore> MakePortionsIndex(const std::deque<std::shared_ptr<TPortionInfo>>& portions);
    bool IsExclusiveInterval(const ui64 portionId) const;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterRequestResourcesAllocated, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(TEvBordersConstructionResult, Handle);
            hFunc(TEvMergeBordersResult, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr&);
    void Handle(const TEvBordersConstructionResult::TPtr&);
    void Handle(const TEvMergeBordersResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&);
    void AbortAndPassAway(const TString& error);
    std::map<ui32, std::shared_ptr<arrow::Field>> GetFetchingColumns() const;

public:
    TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
