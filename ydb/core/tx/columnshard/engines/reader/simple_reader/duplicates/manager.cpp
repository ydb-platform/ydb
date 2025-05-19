#include "fetching.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/source_cache.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TInternalFilterConstructor::TInternalFilterConstructor(
    const std::shared_ptr<IFilterSubscriber>& callback, const std::shared_ptr<IDataSource>& source)
    : Callback(callback)
    , RowsCount(source->GetRecordsCount()) {
    AFL_VERIFY(!!Callback);
    AFL_VERIFY(RowsCount);
}

TDuplicateFilterConstructor::TDuplicateFilterConstructor(const TSpecialReadContext& context)
    : TActor(&TDuplicateFilterConstructor::StateMain)
    , SourceCache([this]() {
        TSourceCache* cache = new TSourceCache();
        RegisterWithSameMailbox((IActor*)cache);
        return cache;
    }())
    , Intervals([&context]() {
        std::remove_const_t<decltype(TDuplicateFilterConstructor::Intervals)> intervals;
        const auto& portions = context.GetReadMetadata()->SelectInfo->Portions;
        for (ui64 i = 0; i < portions.size(); ++i) {
            const auto& portion = portions[i];
            intervals.Insert({ portion->IndexKeyStart(), portion->IndexKeyEnd() }, TSourceInfo(i, portion));
        }
        return intervals;
    }())
    , FiltersCache(100)   // FIXME configure
{
}

void TDuplicateFilterConstructor::Handle(const TEvRequestFilter::TPtr& ev) {
    auto source = std::dynamic_pointer_cast<TPortionDataSource>(ev->Get()->GetSource());
    AFL_VERIFY(source);

    std::vector<std::shared_ptr<IDataSource>> sourcesToFetch;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    {
        const auto collector = [&sourcesToFetch, &borders, context = ev->Get()->GetSource()->GetContextAsVerified<TSpecialReadContext>()](
                                   const TInterval<NArrow::TSimpleRow>& /*interval*/, const TSourceInfo& info) {
            auto source = info.Construct(context);
            sourcesToFetch.emplace_back(source);
            borders.emplace(
                source->GetSourceId(), NArrow::TFirstLastSpecialKeys(source->GetPortionInfo().IndexKeyStart(),
                                           source->GetPortionInfo().IndexKeyEnd(), source->GetPortionInfo().IndexKeyStart().GetSchema()));
        };
        Intervals.FindIntersections(source->GetPortionInfo().IndexKeyStart(), source->GetPortionInfo().IndexKeyEnd(), collector);
    }

    LOCAL_LOG_TRACE("event", "request_filter")("source", source->GetSourceId())("fetching_sources", sourcesToFetch.size());
    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1) {
        // FIXME: if only filtration by snapshot not needed
        AFL_VERIFY(sourcesToFetch.front()->GetSourceId() == source->GetSourceId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, sourcesToFetch.front()->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    TColumnDataSplitter splitter(borders, NArrow::TFirstLastSpecialKeys(source->GetPortionInfo().IndexKeyStart(),
                                              source->GetPortionInfo().IndexKeyEnd(), source->GetPortionInfo().IndexKeyStart().GetSchema()));

    SourceCache->GetSourcesData(std::move((std::vector<std::shared_ptr<IDataSource>>)sourcesToFetch), ev->Get()->GetSource()->GetGroupGuard(),
        std::make_unique<TSourceDataSubscriber>(
            SelfId(), source, std::make_shared<TInternalFilterConstructor>(ev->Get()->GetSubscriber(), source), std::move(splitter)));
}

void TDuplicateFilterConstructor::Handle(const TEvConstructFilters::TPtr& ev) {
    const auto& mainSource = ev->Get()->GetSource();
    const TColumnDataSplitter& splitter = ev->Get()->GetSplitter();
    const TSnapshot maxVersion = mainSource->GetContext()->GetReadMetadata()->GetRequestSnapshot();

    THashMap<ui64, std::vector<std::optional<TColumnDataSplitter::TSourceSegment>>> splitted;
    for (const auto& [id, data] : ev->Get()->GetColumnData()) {
        splitted[id] = splitter.SplitPortion(data, id, maxVersion);
    }

    THashSet<ui64> builtIntervals;
    {
        const auto& splittedMain = *TValidator::CheckNotNull(splitted.FindPtr(mainSource->GetSourceId()));
        AFL_VERIFY(splittedMain.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            if (!splittedMain[i]) {
                builtIntervals.insert(i);
            } else if (auto* findBuilding = BuildingFilters.FindPtr(splittedMain[i]->GetInterval())) {
                findBuilding->emplace_back(ev->Get()->GetCallback());
                builtIntervals.insert(i);
            } else if (auto findCached = FiltersCache.Find(splittedMain[i]->GetInterval()); findCached != FiltersCache.End()) {
                ev->Get()->GetCallback()->AddFilter(findCached.Key(), findCached.Value());
                builtIntervals.insert(i);
            } else {
                BuildingFilters[splittedMain[i]->GetInterval()].emplace_back(ev->Get()->GetCallback());
            }
        }
    }
    LOCAL_LOG_TRACE("event", "construct_filters")
    ("source", mainSource->GetSourceId())("built_intervals", builtIntervals.size())("intervals", splitter.NumIntervals());
    if (ev->Get()->GetCallback()->IsDone()) {
        return;
    }

    std::vector<THashMap<ui64, TColumnDataSplitter::TSourceSegment>> intervals(splitter.NumIntervals());
    for (auto&& [source, portionIntervals] : splitted) {
        for (ui64 i = 0; i < portionIntervals.size(); ++i) {
            if (portionIntervals[i]) {
                intervals[i].emplace(source, std::move(*portionIntervals[i]));
            }
        }
    }

    NArrow::NMerger::TCursor maxVersionBatch = [snapshot = mainSource->GetContext()->GetReadMetadata()->GetRequestSnapshot()]() {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, std::numeric_limits<ui64>::max());
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    }();

    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        auto&& segments = intervals[i];
        if (segments.empty() || builtIntervals.contains(i)) {
            // Do nothing
        } else if (segments.size() == 1) {
            // FIXME: if only filtration by snapshot not needed
            const auto mapInfo = segments.begin()->second.GetInterval();
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, mapInfo.GetRowsCount());
            AFL_VERIFY(BuildingFilters.contains(mapInfo));
            Send(SelfId(), new TEvFiltersConstructed(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mapInfo, filter } })));
        } else {
            THashMap<ui64, TDuplicateMapInfo> mapInfos;
            for (auto&& [source, segment] : segments) {
                mapInfos.emplace(source, segment.GetInterval());
            }
            const std::shared_ptr<TBuildDuplicateFilters> task =
                std::make_shared<TBuildDuplicateFilters>(mainSource->GetContext()->GetReadMetadata()->GetReplaceKey(),
                    IIndexInfo::GetSnapshotColumnNames(), mainSource->GetContext()->GetCommonContext()->GetCounters(), maxVersionBatch,
                    std::make_unique<TFilterResultSubscriber>(SelfId(), std::move(mapInfos)));
            for (auto&& [source, segment] : segments) {
                task->AddSource(std::make_shared<NArrow::TGeneralContainer>(segment.ExtractData()),
                    std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter()), source);
                Y_UNUSED(BuildingFilters.emplace(segment.GetInterval(), std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
            }
            NConveyor::TScanServiceOperator::SendTaskToExecute(task, mainSource->GetContext()->GetCommonContext()->GetConveyorProcessId());
        }
    }

    // FIXME: mem guard lost
}

void TDuplicateFilterConstructor::Handle(const TEvFiltersConstructed::TPtr& ev) {
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

std::shared_ptr<TPortionDataSource> TDuplicateFilterConstructor::TSourceInfo::Construct(
    const std::shared_ptr<TSpecialReadContext>& context) const {
    const auto& portions = context->GetReadMetadata()->SelectInfo->Portions;
    AFL_VERIFY(SourceIdx < portions.size());
    return std::make_shared<TPortionDataSource>(SourceIdx, portions[SourceIdx], context);
}

void TDuplicateFilterConstructor::TSourceDataSubscriber::OnSourcesReady(TSourceCache::TSourcesData&& result) {
    TActorContext::AsActorContext().Send(Owner, new TEvConstructFilters(Source, Callback, std::move(result), std::move(Splitter)));
}

void TDuplicateFilterConstructor::TFilterResultSubscriber::OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) {
    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;
    for (const auto& [source, filter] : result) {
        filters.emplace(*TValidator::CheckNotNull(InfoBySource.FindPtr(source)), filter);
    }
    TActorContext::AsActorContext().Send(Owner, new TEvFiltersConstructed(std::move(filters)));
}

}   // namespace NKikimr::NOlap::NReader::NSimple
