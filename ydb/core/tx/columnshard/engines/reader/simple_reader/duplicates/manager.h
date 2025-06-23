#pragma once

#include "common.h"
#include "context.h"
#include "events.h"
#include "interval_tree.h"
#include "private_events.h"
#include "source_cache.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;
}   // namespace NKikimr::NOlap::NReader::NSimple

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TDuplicateManager: public NActors::TActor<TDuplicateManager> {
private:
    inline static TAtomicCounter NextRequestId = 0;

    NColumnShard::TDuplicateFilteringCounters Counters;
    const THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    const TIntervalTree<NArrow::TSimpleRow, ui64> Intervals;
    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter> FiltersCache;
    THashMap<TDuplicateMapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>> BuildingFilters;
    TSourceCache Fetcher;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NPrivate::TEvDuplicateFilterDataFetched, Handle);
            hFunc(NPrivate::TEvDuplicateSourceCacheResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NPrivate::TEvDuplicateFilterDataFetched::TPtr&);
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
        AFL_VERIFY(portion);
        return *portion;
    }

    ui64 MakeRequestId() {
        return NextRequestId.Inc();
    }

public:
    TDuplicateManager(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
