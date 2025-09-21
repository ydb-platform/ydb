#include "manager.h"
#include "splitter.h"

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

    TBuildFilterContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(
                Request.GetOwner(), new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(errorAddresses.GetErrorMessage())));
            return;
        }

        AFL_VERIFY(AllocationGuard);
        const std::shared_ptr<TBuildDuplicateFilters> task =
            std::make_shared<TBuildDuplicateFilters>(std::move(Request), std::move(objectAddresses), std::move(AllocationGuard));
        NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
    }

    virtual bool DoIsAborted() const override {
        return Request.GetContext()->GetRequest()->Get()->GetAbortionFlag() &&
               Request.GetContext()->GetRequest()->Get()->GetAbortionFlag()->Val();
    }

public:
    TColumnFetchingCallback(TBuildFilterContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Request(std::move(request))
        , AllocationGuard(allocationGuard)
    {
        AFL_VERIFY(allocationGuard);
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Request.GetOwner());
        TActorContext::AsActorContext().Send(
            Request.GetOwner(), new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(errorMessage)));
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request.GetContext()->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        THashSet<TPortionAddress> portionAddresses;
        for (const auto& [portionId, portion] : Request.GetRequiredPortions()) {
            AFL_VERIFY(portionAddresses.emplace(portion->GetAddress()).second);
        }
        auto columnDataManager = Request.GetColumnDataManager();
        auto columns = Request.GetFetchingColumnIds();
        columnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, portionAddresses, std::move(columns),
            std::make_shared<TColumnFetchingCallback>(std::move(Request), guard));
        return true;
    }

public:
    TColumnDataAllocation(TBuildFilterContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    TBuildFilterContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsMemoryGuard;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        if (result.HasErrors()) {
            Request.GetContext()->Abort(result.GetErrorMessage());
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Request.GetFetchingColumnIds(), false);
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(Request.GetRequestGuard()->GetMemoryProcessId(),
            Request.GetRequestGuard()->GetMemoryScopeId(), Request.GetRequestGuard()->GetMemoryGroupId(),
            { std::make_shared<TColumnDataAllocation>(std::move(Request), mem) }, (ui64)TFilterAccumulator::EFetchingStage::COLUMN_DATA);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Request.GetContext()->GetRequest()->Get()->GetAbortionFlag();
    }

public:
    TColumnDataAccessorFetching(
        TBuildFilterContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& accessorsMemoryGuard)
        : Request(std::move(request))
        , AccessorsMemoryGuard(accessorsMemoryGuard)
    {
    }

    static ui64 GetRequiredMemory(const TBuildFilterContext& request, const std::shared_ptr<ISnapshotSchema>& schema) {
        TDataAccessorsRequest dataAccessorsRequest(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const auto& [portionId, portion] : request.GetRequiredPortions()) {
            dataAccessorsRequest.AddPortion(portion);
        }
        return dataAccessorsRequest.PredictAccessorsMemory(schema);
    }
};

class TDataAccessorAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request.GetContext()->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const auto& [portionId, portion] : Request.GetRequiredPortions()) {
            request->AddPortion(portion);
        }
        auto dataAccessorsManager = Request.GetDataAccessorsManager();
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(std::move(Request), guard));
        dataAccessorsManager->AskData(request);
        return true;
    }

public:
    TDataAccessorAllocation(TBuildFilterContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TPortionIntersectionsAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TActorId Owner;
    std::shared_ptr<TFilterAccumulator> Request;
    YDB_READONLY_DEF(std::unique_ptr<TFilterBuildingGuard>, RequestGuard);

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvFilterRequestResourcesAllocated(Request, guard, std::move(RequestGuard)));
        return true;
    }

public:
    TPortionIntersectionsAllocation(const TActorId& owner, const std::shared_ptr<TFilterAccumulator>& request, const ui64 mem,
        std::unique_ptr<TFilterBuildingGuard>&& requestGuard)
        : NGroupedMemoryManager::IAllocation(mem)
        , Owner(owner)
        , Request(request)
        , RequestGuard(std::move(requestGuard))
    {
    }
};
}   // namespace

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, const std::deque<NSimple::TSourceConstructor>& portions)
    : TActor(&TDuplicateManager::StateMain)
    , LastSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema())
    , PKColumns(context.GetPKColumns())
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Intervals(MakeIntervalTree(portions))
    , Portions(MakePortionsIndex(Intervals))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
    , FiltersCache(FILTER_CACHE_SIZE)
    , MaterializedBordersCache(BORDER_CACHE_SIZE_COUNT)
{
}

bool TDuplicateManager::IsExclusiveInterval(const NArrow::TSimpleRow& begin, const NArrow::TSimpleRow& end) const {
    ui64 intersectionsCount = 0;
    return Intervals.EachIntersection(TPortionIntervalTree::TRange(begin, true, end, true),
        [&intersectionsCount](const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& /*portion*/) {
            ++intersectionsCount;
            return intersectionsCount == 1;
        });
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    auto constructor = std::make_shared<TFilterAccumulator>(ev);
    TPortionInfo::TConstPtr mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetSourceId());
    if (IsExclusiveInterval(mainPortion->IndexKeyStart(), mainPortion->IndexKeyEnd())) {
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainPortion->GetRecordsCount());
        constructor->SetIntervalsCount(1);
        constructor->AddFilter(0, std::move(filter));
        AFL_VERIFY(constructor->IsDone());
        Counters->OnFilterRequest(1);
        Counters->OnRowsMerged(0, 0, mainPortion->GetRecordsCount());
        return;
    }

    auto task = std::make_shared<TPortionIntersectionsAllocation>(
        SelfId(), constructor, TBuildFilterContext::GetApproximateDataSize(ExpectedIntersectionCount), std::make_unique<TFilterBuildingGuard>());
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(task->GetRequestGuard()->GetMemoryProcessId(),
        task->GetRequestGuard()->GetMemoryScopeId(), task->GetRequestGuard()->GetMemoryGroupId(), { task },
        (ui64)TFilterAccumulator::EFetchingStage::INTERSECTIONS);
    return;
}

void TDuplicateManager::StartIntervalProcessing(const THashMap<ui64, TPortionInfo::TConstPtr>& intersectingPortions,
    const std::shared_ptr<TFilterAccumulator>& constructor, THashSet<ui64>& portionIdsToFetch,
    std::vector<std::pair<TColumnDataSplitter::TBorder, TColumnDataSplitter::TBorder>>& intervalsToBuild) {
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetSourceId());
    THashMap<ui64, TSortableBorders> materializedBorders;
    for (const auto& [portionId, _] : intersectingPortions) {
        materializedBorders.emplace(portionId, GetBorders(portionId));
    }
    TColumnDataSplitter splitter(materializedBorders);
    LOCAL_LOG_TRACE("event", "split_portion")
    ("source", constructor->GetRequest()->Get()->GetSourceId())("splitter", splitter.DebugString())(
        "intersection_portions", intersectingPortions.size());
    THashMap<ui32, NArrow::TColumnFilter> readyFilters;
    {
        ui64 nextIntervalIdx = 0;
        auto scheduleInterval = [&](const TColumnDataSplitter::TBorder& begin, const TColumnDataSplitter::TBorder& end,
                                    const THashSet<ui64>& portions) {
            ++nextIntervalIdx;
            TIntervalBordersView intervalView(begin.MakeView(), end.MakeView());
            if (auto findCached = FiltersCache.Find(
                    TDuplicateMapInfo(constructor->GetRequest()->Get()->GetMaxVersion(), intervalView, mainPortion->GetPortionId()));
                findCached != FiltersCache.End()) {
                AFL_VERIFY(readyFilters.emplace(nextIntervalIdx - 1, findCached.Value()).second);
                Counters->OnFilterCacheHit();
                return true;
            }
            auto [inFlight, emplaced] = IntervalsInFlight.emplace(intervalView, THashMap<ui64, std::vector<TIntervalFilterCallback>>());
            AFL_VERIFY(!inFlight->second.contains(mainPortion->GetPortionId()));
            inFlight->second[mainPortion->GetPortionId()].emplace_back(TIntervalFilterCallback(nextIntervalIdx - 1, constructor));
            if (emplaced) {
                intervalsToBuild.emplace_back(begin, end);
                portionIdsToFetch.insert(portions.begin(), portions.end());
                Counters->OnFilterCacheMiss();
            } else {
                Counters->OnFilterCacheHit();
            }
            return true;
        };
        splitter.ForEachInterval(std::move(scheduleInterval), mainPortion->GetPortionId());
        constructor->SetIntervalsCount(nextIntervalIdx);
    }
    for (auto&& [idx, filter] : std::move(readyFilters)) {
        constructor->AddFilter(idx, std::move(filter));
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TFilterAccumulator> constructor = ev->Get()->GetRequest();
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> memoryGuard = ev->Get()->ExtractAllocationGuard();
    auto requestGuard = ev->Get()->ExtractRequestGuard();

    THashMap<ui64, TPortionInfo::TConstPtr> intersectingPortions;
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetSourceId());
    {
        const auto collector = [&intersectingPortions](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            AFL_VERIFY(intersectingPortions.emplace(portion->GetPortionId(), portion).second);
            return true;
        };
        Intervals.EachIntersection(
            TPortionIntervalTree::TRange(mainPortion->IndexKeyStart(), true, mainPortion->IndexKeyEnd(), true), collector);
    }
    Counters->OnFilterRequest(intersectingPortions.size());
    ExpectedIntersectionCount = intersectingPortions.size();

    LOCAL_LOG_TRACE("event", "request_filter")
    ("source", constructor->GetRequest()->Get()->GetSourceId())("intersecting_portions", intersectingPortions.size());
    AFL_VERIFY(intersectingPortions.size());

    THashSet<ui64> portionIdsToFetch;
    std::vector<std::pair<TColumnDataSplitter::TBorder, TColumnDataSplitter::TBorder>> intervalsToBuild;
    StartIntervalProcessing(intersectingPortions, constructor, portionIdsToFetch, intervalsToBuild);

    THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
    for (const auto& id : portionIdsToFetch) {
        portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
    }

    if (portionIdsToFetch.size()) {
        AFL_VERIFY(!constructor->IsDone());
        AFL_VERIFY(portionIdsToFetch.contains(mainPortion->GetPortionId()))("main_portion", mainPortion->GetPortionId())(
            "required_portions", JoinSeq(',', portionIdsToFetch));
        TBuildFilterContext columnFetchingRequest(SelfId(), constructor, std::move(portionsToFetch), std::move(intervalsToBuild),
            GetFetchingColumns(), PKSchema, ColumnDataManager, DataAccessorsManager, Counters, std::move(requestGuard), memoryGuard);
        memoryGuard->Update(columnFetchingRequest.GetDataSize());
        const ui64 mem = TColumnDataAccessorFetching::GetRequiredMemory(columnFetchingRequest, LastSchema);
        const ui64 processId = columnFetchingRequest.GetRequestGuard()->GetMemoryProcessId();
        const ui64 scopeId = columnFetchingRequest.GetRequestGuard()->GetMemoryScopeId();
        const ui64 groupId = columnFetchingRequest.GetRequestGuard()->GetMemoryGroupId();
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId,
            { std::make_shared<TDataAccessorAllocation>(std::move(columnFetchingRequest), mem) },
            (ui64)TFilterAccumulator::EFetchingStage::ACCESSORS);
    }
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->GetConclusion().GetErrorMessage());
        AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
        return;
    }
    LOCAL_LOG_TRACE("event", "filters_constructed")("filters", ev->Get()->GetConclusion().GetResult().size());
    ui64 callbacksCalled = 0;
    for (auto&& [mapInfo, filter] : ev->Get()->ExtractResult()) {
        if (auto findInterval = IntervalsInFlight.find(mapInfo.GetInterval()); findInterval != IntervalsInFlight.end()) {
            if (auto findPortion = findInterval->second.find(mapInfo.GetSourceId()); findPortion != findInterval->second.end()) {
                for (auto& callback : findPortion->second) {
                    callback.OnFilterReady(filter);
                    ++callbacksCalled;
                }
                findInterval->second.erase(findPortion);
            }
            if (findInterval->second.empty()) {
                IntervalsInFlight.erase(findInterval);
            }
        }
        FiltersCache.Insert(mapInfo, filter);
        LOCAL_LOG_TRACE("event", "extract_constructed_filter")("range", mapInfo.DebugString());
    }
    AFL_VERIFY(callbacksCalled);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
