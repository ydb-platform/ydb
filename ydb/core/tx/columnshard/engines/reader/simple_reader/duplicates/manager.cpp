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

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context)
    : TActor(&TDuplicateManager::StateMain)
    , Counters(context.GetCommonContext()->GetDuplicateFilteringCounters())
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
    , Fetcher(context.GetCommonContext(), SelfId()) {
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<std::shared_ptr<TPortionInfo>> sourcesToFetch;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(ev->Get()->GetSourceId());
    {
        const auto collector = [this, &sourcesToFetch, &borders](const TInterval<NArrow::TSimpleRow>& /*interval*/, const ui64 portionId) {
            const std::shared_ptr<TPortionInfo>& intersectingSource = GetPortionVerified(portionId);
            sourcesToFetch.emplace_back(intersectingSource);
            borders.emplace(
                intersectingSource->GetPortionId(), NArrow::TFirstLastSpecialKeys(intersectingSource->IndexKeyStart(),
                                                        intersectingSource->IndexKeyEnd(), intersectingSource->IndexKeyStart().GetSchema()));
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
    std::shared_ptr<TInternalFilterConstructor> constructor = std::make_shared<TInternalFilterConstructor>(ev, std::move(splitter));

    Fetcher.GetSourcesData(std::move(sourcesToFetch), constructor);
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr& ev) {
    const std::shared_ptr<TInternalFilterConstructor>& context = ev->Get()->GetContext();
    const TEvRequestFilter* filterRequest = context->GetRequest()->Get();
    const std::shared_ptr<TPortionInfo>& requestedPortion = GetPortionVerified(filterRequest->GetSourceId());
    const TColumnDataSplitter& splitter = context->GetIntervals();
    const TSnapshot maxVersion = context->GetRequest()->Get()->GetMaxVersion();

    THashMap<ui64, std::vector<TDuplicateMapInfo>> splitted;
    for (const auto& [id, data] : ev->Get()->GetColumnData()) {
        splitted[id] = splitter.SplitPortion(data, id, maxVersion);
    }

    THashSet<ui64> builtIntervals;
    {
        const auto& splittedMain = *TValidator::CheckNotNull(splitted.FindPtr(filterRequest->GetSourceId()));
        AFL_VERIFY(splittedMain.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            if (!splittedMain[i].GetRowsCount()) {
                builtIntervals.insert(i);
            } else if (auto* findBuilding = BuildingFilters.FindPtr(splittedMain[i])) {
                findBuilding->emplace_back(context);
                builtIntervals.insert(i);
            } else if (auto findCached = FiltersCache.Find(splittedMain[i]); findCached != FiltersCache.End()) {
                context->AddFilter(findCached.Key(), findCached.Value());
                builtIntervals.insert(i);
            } else {
                BuildingFilters[splittedMain[i]].emplace_back(context);
            }
        }
    }
    LOCAL_LOG_TRACE("event", "construct_filters")
    ("source", filterRequest->GetSourceId())("built_intervals", builtIntervals.size())("intervals", splitter.NumIntervals());
    if (context->IsDone()) {
        return;
    }

    std::vector<THashMap<ui64, TDuplicateMapInfo>> intervals(splitter.NumIntervals());
    for (auto&& [source, portionIntervals] : splitted) {
        for (ui64 i = 0; i < portionIntervals.size(); ++i) {
            if (portionIntervals[i].GetRowsCount()) {
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
            Send(SelfId(),
                new NPrivate::TEvFilterConstructionResult(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mapInfo, filter } })));
        } else {
            if (!context->IsProcessStarted()) {
                context->StartProcess(MakeRequestId());
            }
            THashMap<ui64, TDuplicateMapInfo> mapInfos;
            for (auto&& [source, segment] : segments) {
                mapInfos.emplace(source, segment);
            }
            const TColumnDataSplitter::TBorder& finish = splitter.GetIntervalFinish(i);
            const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
                finish.GetKey().GetSchema(), maxVersionBatch, finish.GetKey(), finish.GetIsLast(), Counters, SelfId());
            for (auto&& [source, segment] : segments) {
                const auto* columnData = ev->Get()->GetColumnData().FindPtr(source);
                AFL_VERIFY(columnData)("source", source);
                task->AddSource(*columnData, segment);
                Y_UNUSED(BuildingFilters.emplace(segment, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
            }
            NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task, context->GetConveyorProcessId());
        }
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        return AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
    }
    for (auto&& [key, filter] : ev->Get()->ExtractResult()) {
        auto findWaiting = BuildingFilters.find(key);
        AFL_VERIFY(findWaiting != BuildingFilters.end());
        for (const auto& callback : findWaiting->second) {
            callback->AddFilter(key, std::move(filter));
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateFilterDataFetched::TPtr& ev) {
    Fetcher.OnFetchingResult(ev);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
