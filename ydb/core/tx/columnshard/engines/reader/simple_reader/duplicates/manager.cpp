#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {
class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    TActorId Owner;
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);
    std::vector<TPortionInfo::TConstPtr> Portions;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, errorAddresses.GetErrorMessage()));
            return;
        }

        AFL_VERIFY(AllocationGuard);
        TActorContext::AsActorContext().Send(
            Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, std::move(objectAddresses), std::move(AllocationGuard)));
    }

    virtual bool DoIsAborted() const override {
        return false;
    }

public:
    TColumnFetchingCallback(
        const TActorId& owner, std::shared_ptr<TInternalFilterConstructor>&& context, const std::vector<TPortionInfo::TConstPtr>& portions)
        : Owner(owner)
        , Context(std::move(context))
        , Portions(portions)
    {
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Owner);
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvDuplicateSourceCacheResult(Context, errorMessage));
    }

    void SetAllocationGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard) {
        AFL_VERIFY(!AllocationGuard);
        AllocationGuard = allocationGuard;
        AFL_VERIFY(AllocationGuard);
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    std::shared_ptr<TColumnFetchingCallback> Callback;
    THashSet<TPortionAddress> Portions;
    std::set<ui32> Columns;
    std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Callback->OnError(errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        Callback->SetAllocationGuard(std::move(guard));
        ColumnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, Portions, Columns, std::move(Callback));
        return true;
    }

public:
    TColumnDataAllocation(const std::shared_ptr<TColumnFetchingCallback>& callback, const THashSet<TPortionAddress>& portions,
        const std::set<ui32>& columns, const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Callback(callback)
        , Portions(portions)
        , Columns(columns)
        , ColumnDataManager(columnDataManager)
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<TColumnFetchingCallback> Callback;
    std::shared_ptr<NColumnFetching::TColumnDataManager> ColumnDataManager;
    THashSet<TPortionAddress> Portions;
    std::set<ui32> Columns;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(Callback);
        if (result.HasErrors()) {
            Callback->OnError(result.GetErrorMessage());
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Columns);
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(Callback->GetContext()->GetMemoryProcessId(),
            Callback->GetContext()->GetMemoryScopeId(), Callback->GetContext()->GetMemoryGroupId(),
            { std::make_shared<TColumnDataAllocation>(Callback, Portions, Columns, ColumnDataManager, mem) }, std::nullopt);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }

public:
    TColumnDataAccessorFetching(const std::shared_ptr<TColumnFetchingCallback>& callback,
        const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager, const THashSet<TPortionAddress>& portions,
        const std::set<ui32>& columns)
        : Callback(callback)
        , ColumnDataManager(columnDataManager)
        , Portions(portions)
        , Columns(columns)
    {
    }
};
}   // namespace

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, const std::deque<NSimple::TSourceConstructor>& portions)
    : TActor(&TDuplicateManager::StateMain)
    , PKColumns(context.GetPKColumns())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Intervals([&portions]() {
        TPortionIntervalTree intervals;
        for (const auto& portion : portions) {
            intervals.AddRange(TPortionIntervalTree::TOwnedRange(portion.GetPortion()->IndexKeyStart(), true,
                                   portion.GetPortion()->IndexKeyEnd(), true), portion.GetPortion());
        }
        return intervals;
    }())
    , Portions([this]() {
        THashMap<ui64, std::shared_ptr<TPortionInfo>> portions;
        Intervals.EachRange(
            [&portions](const TPortionIntervalTree::TOwnedRange& /*range*/, const std::shared_ptr<TPortionInfo>& portion) mutable {
                AFL_VERIFY(portions.emplace(portion->GetPortionId(), portion).second);
            });
        return portions;
    }())
    , FiltersCache(100)
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<TPortionInfo::TConstPtr> sourcesToFetch;
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(ev->Get()->GetSourceId());
    {
        const auto collector = [&sourcesToFetch, &borders](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            sourcesToFetch.emplace_back(portion);
            borders.emplace(portion->GetPortionId(),
                NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
        };
        Intervals.EachIntersection(TPortionIntervalTree::TRange(source->IndexKeyStart(), true, source->IndexKeyEnd(), true), collector);
    }

    LOCAL_LOG_TRACE("event", "request_filter")("source", ev->Get()->GetSourceId())("fetching_sources", sourcesToFetch.size());
    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1 && ev->Get()->GetMaxVersion() >= source->RecordSnapshotMax(ev->Get()->GetMaxVersion())) {
        AFL_VERIFY((*sourcesToFetch.begin())->GetPortionId() == ev->Get()->GetSourceId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, (*sourcesToFetch.begin())->GetRecordsCount());
        ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        return;
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(source->IndexKeyStart(), source->IndexKeyEnd(), source->IndexKeyStart().GetSchema()));
    std::shared_ptr<TInternalFilterConstructor> constructor = std::make_shared<TInternalFilterConstructor>(ev, std::move(splitter));

    {
        THashSet<TPortionAddress> portionAddresses;
        for (const auto& portion : sourcesToFetch) {
            portionAddresses.insert(portion->GetAddress());
        }
        std::set<ui32> columns;
        for (const auto& [columnId, _] : GetFetchingColumns()) {
            columns.emplace(columnId);
        }

        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(
            std::make_shared<TColumnFetchingCallback>(SelfId(), std::move(constructor), sourcesToFetch), ColumnDataManager, portionAddresses,
            columns));
        for (auto&& source : sourcesToFetch) {
            request->AddPortion(source);
        }
        DataAccessorsManager->AskData(request);
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        const TString& error = ev->Get()->GetConclusion().GetErrorMessage();
        ev->Get()->GetContext()->Abort(error);
        AbortAndPassAway(error);
        return;
    }

    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> dataByPortion =
        ev->Get()->ExtractResult().ExtractDataByPortion(GetFetchingColumns());
    const std::shared_ptr<TInternalFilterConstructor>& context = ev->Get()->GetContext();
    const TEvRequestFilter* filterRequest = context->GetRequest()->Get();
    const TColumnDataSplitter& splitter = context->GetIntervals();
    const TSnapshot maxVersion = context->GetRequest()->Get()->GetMaxVersion();
    auto allocationGuard = ev->Get()->ExtractAllocationGuard();

    THashMap<ui64, std::vector<TRowRange>> rangesByPortion;
    for (const auto& [id, data] : dataByPortion) {
        rangesByPortion[id] = splitter.SplitPortion(data);
    }

    THashSet<ui64> builtIntervals;
    {
        const auto& splittedMain = *TValidator::CheckNotNull(rangesByPortion.FindPtr(filterRequest->GetSourceId()));
        AFL_VERIFY(splittedMain.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            TDuplicateMapInfo mapInfo(maxVersion, splittedMain[i], filterRequest->GetSourceId());
            if (!mapInfo.GetRows().NumRows()) {
                builtIntervals.insert(i);
            } else if (auto* findBuilding = BuildingFilters.FindPtr(mapInfo)) {
                AFL_VERIFY(findBuilding->empty())("existing", findBuilding->front()->DebugString())("new", context->DebugString())(
                    "key", mapInfo.DebugString());
                findBuilding->emplace_back(context);
                builtIntervals.insert(i);
            } else if (auto findCached = FiltersCache.Find(mapInfo); findCached != FiltersCache.End()) {
                context->AddFilter(findCached.Key(), findCached.Value());
                builtIntervals.insert(i);
            } else {
                AFL_VERIFY(BuildingFilters.emplace(mapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>({context})).second);
            }
        }
    }
    LOCAL_LOG_TRACE("event", "construct_filters")
    ("source", filterRequest->GetSourceId())("built_intervals", builtIntervals.size())("intervals", splitter.NumIntervals())(
        "done", context->IsDone())("splitter", splitter.DebugString());
    if (context->IsDone()) {
        return;
    }

    std::vector<THashMap<ui64, TRowRange>> intervals(splitter.NumIntervals());
    for (auto&& [source, portionIntervals] : rangesByPortion) {
        for (ui64 i = 0; i < portionIntervals.size(); ++i) {
            if (portionIntervals[i].NumRows()) {
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
        } else if (segments.size() == 1 && maxVersion >= GetPortionVerified(segments.begin()->first)->RecordSnapshotMax(maxVersion)) {
            TDuplicateMapInfo mapInfo(maxVersion, segments.begin()->second, segments.begin()->first);
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, mapInfo.GetRows().NumRows());
            AFL_VERIFY(BuildingFilters.contains(mapInfo));
            Send(SelfId(),
                new NPrivate::TEvFilterConstructionResult(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mapInfo, filter } })));
        } else {
            const TColumnDataSplitter::TBorder& finish = splitter.GetIntervalFinish(i);
            const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
                finish.GetKey().GetSchema(), maxVersionBatch, finish.GetKey(), finish.GetIsLast(), Counters, SelfId());
            for (auto&& [source, segment] : segments) {
                const auto* columnData = dataByPortion.FindPtr(source);
                AFL_VERIFY(columnData)("source", source);
                TDuplicateMapInfo mapInfo(maxVersion, segment, source);
                task->AddSource(*columnData, allocationGuard, mapInfo);
                Y_UNUSED(BuildingFilters.emplace(mapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
            }
            NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
        }
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->GetConclusion().GetErrorMessage());
        return AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
    }
    LOCAL_LOG_TRACE("event", "filters_constructed")("sources", ev->Get()->GetConclusion().GetResult().size());
    for (auto&& [key, filter] : ev->Get()->ExtractResult()) {
        LOCAL_LOG_TRACE("event", "extract_constructed_filter")("range", key.DebugString());
        auto findWaiting = BuildingFilters.find(key);
        AFL_VERIFY(findWaiting != BuildingFilters.end());
        for (const std::shared_ptr<TInternalFilterConstructor>& callback : findWaiting->second) {
            callback->AddFilter(key, std::move(filter));
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
