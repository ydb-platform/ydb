#include "fetching.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/source_cache.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TInternalFilterConstructor::TInternalFilterConstructor(const std::shared_ptr<IFilterSubscriber>& callback, const ui64 rowsCount)
    : Callback(callback)
    , RowsCount(rowsCount) {
    AFL_VERIFY(!!Callback);
    AFL_VERIFY(RowsCount);
}

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context)
    : TActor(&TDuplicateManager::StateMain)
    , Counters(context.GetCommonContext()->GetDuplicateFilteringCounters())
    , SourceCache([this, &context]() {
        TSourceCache* cache = new TSourceCache(context.GetCommonContext());
        RegisterWithSameMailbox((IActor*)cache);
        return cache;
    }())
    , Portions([&context]() {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
        for (const auto& portion : context.GetReadMetadata()->SelectInfo->Portions) {
            portions.emplace(portion->GetPortionId(), portion);
        }
        return portions;
    }())
    , Intervals([&context]() {
        std::remove_const_t<decltype(TDuplicateManager::Intervals)> intervals;
        for (const auto& portion : context.GetReadMetadata()->SelectInfo->Portions) {
            intervals.Insert({ portion->IndexKeyStart(), portion->IndexKeyEnd() }, portion->GetPortionId());
        }
        return intervals;
    }())
    , FiltersCache(100)   // FIXME configure
    , ConveyorProcessGuard(context.GetCommonContext()->GetConveyorProcessGuard()) {
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<std::shared_ptr<TPortionInfo>> sourcesToFetch;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(ev->Get()->GetSourceId());
    {
        const auto collector = [this, &sourcesToFetch, &borders](const TInterval<NArrow::TSimpleRow>& /*interval*/, const ui64 portionId) {
            const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(portionId);
            sourcesToFetch.emplace_back(source);
            borders.emplace(source->GetPortionId(),
                NArrow::TFirstLastSpecialKeys(source->IndexKeyStart(), source->IndexKeyEnd(), source->IndexKeyStart().GetSchema()));
        };
        Intervals.FindIntersections(source->IndexKeyStart(), source->IndexKeyEnd(), collector);
    }

    LOCAL_LOG_TRACE("event", "request_filter")("source", ev->Get()->GetSourceId())("fetching_sources", sourcesToFetch.size());
    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1 && ev->Get()->GetMaxVersion() >= source->RecordSnapshotMax(ev->Get()->GetMaxVersion())) {
        AFL_VERIFY(sourcesToFetch.front()->GetPortionId() == ev->Get()->GetSourceId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, sourcesToFetch.front()->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(source->IndexKeyStart(), source->IndexKeyEnd(), source->IndexKeyStart().GetSchema()));

    SourceCache->GetSourcesData(
        std::move(sourcesToFetch), ev->Get()->GetMemoryGroup(), std::make_unique<TSourceDataSubscriber>(SelfId(), ev, std::move(splitter)));
}

void TDuplicateManager::Handle(const TEvConstructFilters::TPtr& ev) {
    const auto& filterRequest = ev->Get()->GetOriginalRequest()->Get();
    const std::shared_ptr<TPortionInfo>& requestedPortion = GetPortionVerified(filterRequest->GetSourceId());
    const TColumnDataSplitter& splitter = ev->Get()->GetSplitter();
    const TSnapshot maxVersion = ev->Get()->GetOriginalRequest()->Get()->GetMaxVersion();

    THashMap<ui64, std::vector<TDuplicateMapInfo>> splitted;
    for (const auto& [id, data] : ev->Get()->GetColumnData()) {
        splitted[id] = splitter.SplitPortion(data, id, maxVersion);
    }

    std::shared_ptr<TInternalFilterConstructor> constructor =
        std::make_shared<TInternalFilterConstructor>(filterRequest->GetSubscriber(), requestedPortion->GetRecordsCount());

    THashSet<ui64> builtIntervals;
    {
        const auto& splittedMain = *TValidator::CheckNotNull(splitted.FindPtr(filterRequest->GetSourceId()));
        AFL_VERIFY(splittedMain.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            if (!splittedMain[i].GetRowsCount()) {
                builtIntervals.insert(i);
            } else if (auto* findBuilding = BuildingFilters.FindPtr(splittedMain[i])) {
                findBuilding->emplace_back(constructor);
                builtIntervals.insert(i);
            } else if (auto findCached = FiltersCache.Find(splittedMain[i]); findCached != FiltersCache.End()) {
                constructor->AddFilter(findCached.Key(), findCached.Value());
                builtIntervals.insert(i);
            } else {
                BuildingFilters[splittedMain[i]].emplace_back(constructor);
            }
        }
    }
    LOCAL_LOG_TRACE("event", "construct_filters")
    ("source", filterRequest->GetSourceId())("built_intervals", builtIntervals.size())("intervals", splitter.NumIntervals());
    if (constructor->IsDone()) {
        return;
    }

    std::vector<THashMap<ui64, TDuplicateMapInfo>> intervals(splitter.NumIntervals());
    for (auto&& [source, portionIntervals] : splitted) {
        for (ui64 i = 0; i < portionIntervals.size(); ++i) {
            if (portionIntervals[i]) {
                intervals[i].emplace(source, std::move(portionIntervals[i]));
            }
        }
    }

    NArrow::NMerger::TCursor maxVersionBatch = [&maxVersion]() {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, maxVersion, std::numeric_limits<ui64>::max());
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    }();

    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        auto&& segments = intervals[i];
        if (segments.empty() || builtIntervals.contains(i)) {
            // Do nothing
        } else if (segments.size() == 1 && maxVersion >= requestedPortion->RecordSnapshotMax(maxVersion)) {
            const TDuplicateMapInfo& mapInfo = segments.begin()->second;
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, mapInfo.GetRowsCount());
            AFL_VERIFY(BuildingFilters.contains(mapInfo));
            Send(SelfId(), new TEvFiltersConstructed(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mapInfo, filter } })));
        } else {
            THashMap<ui64, TDuplicateMapInfo> mapInfos;
            for (auto&& [source, segment] : segments) {
                mapInfos.emplace(source, segment);
            }
            const TColumnDataSplitter::TBorder& border = splitter.GetIntervalFinish(i);
            const std::shared_ptr<TBuildDuplicateFilters> task =
                std::make_shared<TBuildDuplicateFilters>(border.GetKey().GetSchema(), maxVersionBatch, border.GetKey(), border.GetIsLast(),
                    Counters, std::make_unique<TFilterResultSubscriber>(SelfId(), std::move(mapInfos)));
            for (auto&& [source, segment] : segments) {
                const auto* columnData = ev->Get()->GetColumnData().FindPtr(source);
                AFL_VERIFY(columnData)("source", source);
                task->AddSource(*columnData, segment.GetOffset(), source);
                Y_UNUSED(BuildingFilters.emplace(segment, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
            }
            NConveyor::TScanServiceOperator::SendTaskToExecute(task, ConveyorProcessGuard->GetProcessId());
        }
    }
}

void TDuplicateManager::Handle(const TEvFiltersConstructed::TPtr& ev) {
    for (const auto& [key, filter] : ev->Get()->GetResult()) {
        auto findWaiting = BuildingFilters.find(key);
        AFL_VERIFY(findWaiting != BuildingFilters.end());
        for (const auto& callback : findWaiting->second) {
            callback->AddFilter(key, filter);
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

void TDuplicateManager::TSourceDataSubscriber::OnSourcesReady(TSourceCache::TSourcesData&& result) {
    TActorContext::AsActorContext().Send(Owner, new TEvConstructFilters(OriginalRequest, std::move(result), std::move(Splitter)));
}

void TDuplicateManager::TFilterResultSubscriber::OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) {
    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;
    for (const auto& [source, filter] : result) {
        filters.emplace(*TValidator::CheckNotNull(InfoBySource.FindPtr(source)), filter);
    }
    TActorContext::AsActorContext().Send(Owner, new TEvFiltersConstructed(std::move(filters)));
}

}   // namespace NKikimr::NOlap::NReader::NSimple
