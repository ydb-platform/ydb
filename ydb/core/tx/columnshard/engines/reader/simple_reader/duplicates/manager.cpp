#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {
class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TColumnDataCachePolicy::TAddress;
    using TObject = NGeneralCache::TColumnDataCachePolicy::TObject;

    TActorId Owner;
    TActorId ColumnShardActorId;
    std::shared_ptr<TInternalFilterConstructor> Context;
    std::map<ui32, std::shared_ptr<arrow::Field>> Columns;
    std::vector<TPortionInfo::TConstPtr> Portions;

private:
    virtual void DoOnResultReady(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& /*removedAddresses*/,
        THashMap<TAddress, TString>&& errorAddresses) const override {
        if (!errorAddresses.empty()) {
            TActorContext::AsActorContext().Send(
                Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, TConclusionStatus::Fail(errorAddresses.begin()->second)));
            return;
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& [_, field] : Columns) {
            fields.emplace_back(field);
        }

        THashMap<ui64, std::shared_ptr<TColumnsData>> result;
        std::shared_ptr<NGroupedMemoryManager::TCompositeAllocationGuard> allocationGuard =
            std::make_shared<NGroupedMemoryManager::TCompositeAllocationGuard>();
        for (const TPortionInfo::TConstPtr& portion : Portions) {
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
            for (const auto& [columnId, field] : Columns) {
                NGeneralCache::TColumnData* findColumn =
                    objectAddresses.FindPtr(NGeneralCache::TGlobalColumnAddress(ColumnShardActorId, portion->GetAddress(), columnId));
                AFL_VERIFY(findColumn)("portion", portion->DebugString())("column", columnId);
                columns.emplace_back(findColumn->GetData());
                allocationGuard->Add(findColumn->GetMemoryGuard());
            }
            std::shared_ptr<NArrow::TGeneralContainer> container = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(columns));
            result.emplace(portion->GetPortionId(), std::make_shared<TColumnsData>(std::move(container), std::move(allocationGuard)));
        }
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, std::move(result)));
    }

    virtual bool DoIsAborted() const override {
        return false;
    }

public:
    TColumnFetchingCallback(const TActorId& owner, const TActorId& columnShardActorId, std::shared_ptr<TInternalFilterConstructor>&& context,
        std::map<ui32, std::shared_ptr<arrow::Field>>&& columns, std::vector<TPortionInfo::TConstPtr>&& portions)
        : Owner(owner)
        , ColumnShardActorId(columnShardActorId)
        , Context(std::move(context))
        , Columns(std::move(columns))
        , Portions(std::move(portions))
    {
    }
};
}   // namespace

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context)
    : TActor(&TDuplicateManager::StateMain)
    , ColumnShardActorId(context.GetCommonContext()->GetColumnShardActorId())
    , PKColumns(context.GetPKColumns())
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
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<TPortionInfo::TConstPtr> sourcesToFetch;
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

    std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
    {
        for (const auto& columnId : PKColumns->GetColumnIds()) {
            fieldsByColumn.emplace(columnId, PKColumns->GetFilteredSchemaVerified().GetFieldByColumnIdVerified(columnId));
        }
        for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
            fieldsByColumn.emplace(columnId, IIndexInfo::GetColumnFieldVerified(columnId));
        }
    }

    auto sortingSchema = sourcesToFetch.front()->IndexKeyStart().GetSchema();
    THashSet<NGeneralCache::TColumnDataCachePolicy::TAddress> columnsToFetch;
    for (const auto& portion : sourcesToFetch) {
        for (const auto& [columnId, _] : fieldsByColumn) {
            columnsToFetch.emplace(ColumnShardActorId, portion->GetAddress(), columnId);
        }
    }

    NColumnFetching::TGeneralCache::AskObjects(NBlobOperations::EConsumer::DUPLICATE_FILTERING, std::move(columnsToFetch),
        std::make_shared<TColumnFetchingCallback>(
            SelfId(), ColumnShardActorId, std::move(constructor), std::move(fieldsByColumn), std::move(sourcesToFetch)));
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
        return;
    }

    const std::shared_ptr<TInternalFilterConstructor>& context = ev->Get()->GetContext();
    const TEvRequestFilter* filterRequest = context->GetRequest()->Get();
    const std::shared_ptr<TPortionInfo>& requestedPortion = GetPortionVerified(filterRequest->GetSourceId());
    const TColumnDataSplitter& splitter = context->GetIntervals();
    const TSnapshot maxVersion = context->GetRequest()->Get()->GetMaxVersion();

    THashMap<ui64, std::vector<TDuplicateMapInfo>> splitted;
    for (const auto& [id, data] : *ev->Get()->GetConclusion()) {
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
            THashMap<ui64, TDuplicateMapInfo> mapInfos;
            for (auto&& [source, segment] : segments) {
                mapInfos.emplace(source, segment);
            }
            const TColumnDataSplitter::TBorder& finish = splitter.GetIntervalFinish(i);
            const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
                finish.GetKey().GetSchema(), maxVersionBatch, finish.GetKey(), finish.GetIsLast(), Counters, SelfId());
            for (auto&& [source, segment] : segments) {
                const auto* columnData = ev->Get()->GetConclusion()->FindPtr(source);
                AFL_VERIFY(columnData)("source", source);
                task->AddSource(*columnData, segment);
                Y_UNUSED(BuildingFilters.emplace(segment, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
            }
            NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
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

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
