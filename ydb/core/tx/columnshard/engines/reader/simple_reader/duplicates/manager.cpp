#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/interval_borders.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/merge.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/splitter.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {

class TColumnFetchingRequest: NColumnShard::TMonitoringObjectsCounter<TColumnFetchingRequest>, TMoveOnly {
private:
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);
    YDB_READONLY_DEF(std::set<ui32>, Columns);
    YDB_READONLY_DEF(std::shared_ptr<NColumnFetching::TColumnDataManager>, ColumnDataManager);
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>, DataAccessorsManager);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> IntersectionsMemory;

public:
    TColumnFetchingRequest(const TActorId owner, const std::shared_ptr<TInternalFilterConstructor>& context,
        std::vector<TPortionInfo::TConstPtr>&& portions, const std::set<ui32>& columns,
        const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& insersectionsMemory)
        : Owner(owner)
        , Context(context)
        , Portions(std::move(portions))
        , Columns(columns)
        , ColumnDataManager(columnDataManager)
        , DataAccessorsManager(dataAccessorsManager)
        , IntersectionsMemory(insersectionsMemory)
    {
        AFL_VERIFY(Owner);
        AFL_VERIFY(Context);
        AFL_VERIFY(Portions.size());
        AFL_VERIFY(Columns.size());
        AFL_VERIFY(ColumnDataManager);
        AFL_VERIFY(DataAccessorsManager);
        AFL_VERIFY(IntersectionsMemory);
    }
};

class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    TColumnFetchingRequest Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(
                Request.GetOwner(), new NPrivate::TEvDuplicateSourceCacheResult(Request.GetContext(), errorAddresses.GetErrorMessage()));
            return;
        }

        AFL_VERIFY(AllocationGuard);
        TActorContext::AsActorContext().Send(Request.GetOwner(),
            new NPrivate::TEvDuplicateSourceCacheResult(Request.GetContext(), std::move(objectAddresses), std::move(AllocationGuard)));
    }

    virtual bool DoIsAborted() const override {
        return Request.GetContext()->GetRequest()->Get()->GetAbortionFlag() &&
               Request.GetContext()->GetRequest()->Get()->GetAbortionFlag()->Val();
    }

public:
    TColumnFetchingCallback(TColumnFetchingRequest&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Request(std::move(request))
        , AllocationGuard(allocationGuard)
    {
        AFL_VERIFY(allocationGuard);
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Request.GetOwner());
        TActorContext::AsActorContext().Send(
            Request.GetOwner(), new NPrivate::TEvDuplicateSourceCacheResult(Request.GetContext(), errorMessage));
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TColumnFetchingRequest Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request.GetContext()->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        THashSet<TPortionAddress> portionAddresses;
        for (const auto& portion : Request.GetPortions()) {
            AFL_VERIFY(portionAddresses.emplace(portion->GetAddress()).second);
        }
        auto columnDataManager = Request.GetColumnDataManager();
        auto columns = Request.GetColumns();
        columnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, portionAddresses, std::move(columns),
            std::make_shared<TColumnFetchingCallback>(std::move(Request), guard));
        return true;
    }

public:
    TColumnDataAllocation(TColumnFetchingRequest&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    TColumnFetchingRequest Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsMemoryGuard;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        if (result.HasErrors()) {
            Request.GetContext()->Abort(result.GetErrorMessage());
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Request.GetColumns(), false);
        }

        auto context = Request.GetContext();
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(context->GetMemoryProcessId(), context->GetMemoryScopeId(),
            context->GetMemoryGroupId(), { std::make_shared<TColumnDataAllocation>(std::move(Request), mem) },
            (ui64)TInternalFilterConstructor::EFetchingStage::COLUMN_DATA);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Request.GetContext()->GetRequest()->Get()->GetAbortionFlag();
    }

public:
    TColumnDataAccessorFetching(
        TColumnFetchingRequest&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& accessorsMemoryGuard)
        : Request(std::move(request))
        , AccessorsMemoryGuard(accessorsMemoryGuard)
    {
    }

    static ui64 GetRequiredMemory(const TColumnFetchingRequest& request, const std::shared_ptr<ISnapshotSchema>& schema) {
        TDataAccessorsRequest dataAccessorsRequest(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (auto&& portion : request.GetPortions()) {
            dataAccessorsRequest.AddPortion(portion);
        }
        return dataAccessorsRequest.PredictAccessorsMemory(schema);
    }
};

class TDataAccessorAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TColumnFetchingRequest Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request.GetContext()->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (auto&& portion : Request.GetPortions()) {
            request->AddPortion(portion);
        }
        auto dataAccessorsManager = Request.GetDataAccessorsManager();
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(std::move(Request), guard));
        dataAccessorsManager->AskData(request);
        return true;
    }

public:
    TDataAccessorAllocation(TColumnFetchingRequest&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TPortionIntersectionsAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TActorId Owner;
    std::shared_ptr<TInternalFilterConstructor> Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request->Abort(TStringBuilder() << "cannot allocate memory: " << errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvFilterRequestResourcesAllocated(Request, guard));
        return true;
    }

public:
    TPortionIntersectionsAllocation(const TActorId& owner, const std::shared_ptr<TInternalFilterConstructor>& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Owner(owner)
        , Request(request)
    {
    }
};

} // namespace

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
{
    const auto& columnShardConfig = AppDataVerified().ColumnShardConfig;
    UsePortionsCache = columnShardConfig.GetDeduplicationCacheEnabled();
    MaxInFlightRequests = columnShardConfig.GetMaxDeduplicationInFlightRequests();
    LoadAdditionalPortions = columnShardConfig.GetDeduplicationLoadAdditionalPortions();
}

void TDuplicateManager::HandleNextRequest() {
    while (!RequestsQueue.empty()) {
        if (MaxInFlightRequests) {
            if (CurrentInFlightRequests >= MaxInFlightRequests) {
                break;
            }
        }

        auto constructor = RequestsQueue.front();
        RequestsQueue.pop();

        ++CurrentInFlightRequests;
        constructor->SetFinishedCallback([actorId = SelfId()]() {
            TActivationContext::AsActorContext().Send(actorId, new NPrivate::TEvFilterBuildFinished());
        });

        auto& request = constructor->GetRequest();
        AFL_VERIFY(request);
        if (UsePortionsCache) {
            if (auto foundPortionCache = PortionsCache.find(request->Get()->GetSourceId()); foundPortionCache != PortionsCache.end()) {
                for (auto& mapInfo : foundPortionCache->second) {
                    if (auto foundFilter = FiltersCache.Find(mapInfo); foundFilter != FiltersCache.End()) {
                        constructor->AddFilter(mapInfo, *foundFilter);
                    }
                }
            }

            if (constructor->IsDone()) {
                Counters->OnFilterPortionsCacheHit();
                continue;
            }

            Counters->OnFilterPortionsCacheMiss();
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(constructor->GetMemoryProcessId(),
            constructor->GetMemoryScopeId(), constructor->GetMemoryGroupId(),
            {std::make_shared<TPortionIntersectionsAllocation>(SelfId(), constructor,
                ExpectedIntersectionCount * sizeof(TPortionInfo::TConstPtr))},
            (ui64)TInternalFilterConstructor::EFetchingStage::INTERSECTIONS);
    }
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    auto constructor = std::make_shared<TInternalFilterConstructor>(ev);
    RequestsQueue.push(constructor);

    HandleNextRequest();
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterBuildFinished::TPtr&) {
    --CurrentInFlightRequests;
    HandleNextRequest();
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TInternalFilterConstructor> constructor = ev->Get()->GetRequest();
    const auto& request = constructor->GetRequest();
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> memoryGuard = ev->Get()->ExtractAllocationGuard();

    std::vector<TPortionInfo::TConstPtr> sourcesToFetch;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(request->Get()->GetSourceId());
    {
        const auto collector = [&sourcesToFetch](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            sourcesToFetch.emplace_back(portion);
        };
        Intervals.EachIntersection(TPortionIntervalTree::TRange(source->IndexKeyStart(), true, source->IndexKeyEnd(), true), collector);
    }
    sourcesToFetch.shrink_to_fit();
    Counters->OnFilterRequest(sourcesToFetch.size());

    LOCAL_LOG_TRACE("event", "request_filter")("source", request->Get()->GetSourceId())("fetching_sources", sourcesToFetch.size());
    AFL_VERIFY(sourcesToFetch.size());
    if (sourcesToFetch.size() == 1) {
        AFL_VERIFY((*sourcesToFetch.begin())->GetPortionId() == source->GetPortionId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, source->GetRecordsCount());
        constructor->AddFilter(TDuplicateMapInfo(request->Get()->GetMaxVersion(), TRowRange(0, source->GetRecordsCount()),
                                   source->GetPortionId()), std::move(filter));
        AFL_VERIFY(constructor->IsDone());
        return;
    }

    auto before = sourcesToFetch.size();
    if (LoadAdditionalPortions) {
        std::vector<TPortionInfo::TConstPtr> additionalSourcesToFetch;
        std::vector<TPortionInfo::TConstPtr> additionalMainSources;
        {
            const auto collector = [&additionalSourcesToFetch](
                                       const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
                additionalSourcesToFetch.emplace_back(portion);
            };
            for (const auto& additionalSource : sourcesToFetch) {
                if (UsePortionsCache && PortionsCache.contains(additionalSource->GetPortionId())) {
                    continue;
                }
                if (additionalSource->GetPortionId() == source->GetPortionId()) {
                    continue;
                }
                additionalMainSources.push_back(additionalSource);
                Intervals.EachIntersection(TPortionIntervalTree::TRange(additionalSource->IndexKeyStart(), true, additionalSource->IndexKeyEnd(), true), collector);
            }
        }

        constructor->SetAdditionalSources(additionalMainSources);
        sourcesToFetch.insert(sourcesToFetch.end(), additionalSourcesToFetch.begin(), additionalSourcesToFetch.end());
        sourcesToFetch.shrink_to_fit();
        std::sort(sourcesToFetch.begin(), sourcesToFetch.end());
        sourcesToFetch.erase(std::unique(sourcesToFetch.begin(), sourcesToFetch.end()), sourcesToFetch.end());
    }
    auto after = sourcesToFetch.size();
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "ADDITIONAL_PORTIONS")("before", before)("after", after);

    ExpectedIntersectionCount = sourcesToFetch.size();
    memoryGuard->Update(sourcesToFetch.size() * sizeof(TPortionInfo::TConstPtr));
    Counters->OnFetchedSources(sourcesToFetch.size());

    std::set<ui32> columns;
    for (const auto& [columnId, _] : GetFetchingColumns()) {
        columns.emplace(columnId);
    }

    TColumnFetchingRequest columnFetchingRequest(
        SelfId(), constructor, std::move(sourcesToFetch), columns, ColumnDataManager, DataAccessorsManager, memoryGuard);
    const ui64 mem = TColumnDataAccessorFetching::GetRequiredMemory(columnFetchingRequest, LastSchema);
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(constructor->GetMemoryProcessId(),
        constructor->GetMemoryScopeId(), constructor->GetMemoryGroupId(),
        { std::make_shared<TDataAccessorAllocation>(std::move(columnFetchingRequest), mem) },
        (ui64)TInternalFilterConstructor::EFetchingStage::ACCESSORS);
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
    auto allocationGuard = ev->Get()->ExtractAllocationGuard();

    LOCAL_LOG_TRACE("event", "construct_filters")("context", context->DebugString());

    auto task = std::make_shared<TFindIntervalBorders>(std::move(dataByPortion),
        Portions, context, std::move(allocationGuard), SelfId());

    NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
}

void TDuplicateManager::Handle(const NPrivate::TEvFindIntervalsResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        const TString& error = ev->Get()->GetConclusion().GetErrorMessage();
        ev->Get()->GetContext()->Abort(error);
        AbortAndPassAway(error);
        return;
    }

    const std::shared_ptr<TInternalFilterConstructor>& context = ev->Get()->GetContext();
    auto allocationGuard = ev->Get()->ExtractAllocationGuard();
    auto dataByPortion = ev->Get()->ExtractDataByPortion();
    auto result = ev->Get()->ExtractResult();
    auto additionalResults = ev->Get()->ExtractAdditionalResults();

    if (LoadAdditionalPortions) {
        for (const auto& [portion, additionalResult] : additionalResults) {
            for (const auto& slice : additionalResult) {
                BuildFilterForSlice(slice, context, allocationGuard, dataByPortion, portion->GetPortionId(), false);
            }
        }
    }

    for (const auto& slice : result) {
        BuildFilterForSlice(slice, context, allocationGuard, dataByPortion, context->GetRequest()->Get()->GetSourceId(), true);
    }
}

void TDuplicateManager::BuildFilterForSlice(const TPortionsSlice& slice, const std::shared_ptr<TInternalFilterConstructor>& constructor,
    const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard,
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
    ui64 portionId, bool constructorForThisSource) {
    const TSnapshot& maxVersion = constructor->GetRequest()->Get()->GetMaxVersion();

    auto findMainRange = slice.GetRangeOptional(portionId);
    if (!findMainRange) {
        return;
    }

    TDuplicateMapInfo mainMapInfo(maxVersion, *findMainRange, portionId);
    if (auto* findBuilding = BuildingFilters.FindPtr(mainMapInfo)) {
        if (constructorForThisSource) {
            AFL_VERIFY(findBuilding->empty())("existing", findBuilding->front()->DebugString())("new", constructor->DebugString())(
                "key", mainMapInfo.DebugString());
            findBuilding->emplace_back(constructor);
            Counters->OnFilterCacheHit();
        }
        return;
    }

    if (auto findCached = FiltersCache.Find(mainMapInfo); findCached != FiltersCache.End()) {
        if (constructorForThisSource) {
            constructor->AddFilter(findCached.Key(), findCached.Value());
        }
        Counters->OnFilterCacheHit();
        return;
    }

    if (slice.GetRanges().size() == 1) {
        if (constructorForThisSource) {
            NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
            filter.Add(true, mainMapInfo.GetRows().NumRows());
            AFL_VERIFY(BuildingFilters.emplace(mainMapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>({constructor})).second);
            Send(SelfId(),
                new NPrivate::TEvFilterConstructionResult(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({{mainMapInfo, filter}})));
            Counters->OnRowsMerged(0, 0, mainMapInfo.GetRows().NumRows());
        }
        return;
    }

    auto getVersionBatch = [](const TSnapshot& snapshot, const ui64 writeId) -> NArrow::NMerger::TCursor {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, writeId);
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    };
    const auto& maxVersionBatch = getVersionBatch(maxVersion, std::numeric_limits<ui64>::max());
    const auto& minUncommittedVersionBatch = getVersionBatch(TSnapshot::Max(), 0);
    const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
        PKSchema, maxVersionBatch, minUncommittedVersionBatch, slice.GetEnd().GetKey(), slice.GetEnd().GetIsLast(), Counters, SelfId());
    for (const auto& [source, segment] : slice.GetRanges()) {
        const auto* columnData = dataByPortion.FindPtr(source);
        AFL_VERIFY(columnData)("source", source);
        TDuplicateMapInfo mapInfo(maxVersion, segment, source);
        task->AddSource(*columnData, allocationGuard, mapInfo);
        AFL_VERIFY(BuildingFilters.emplace(mapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
    }
    NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
    if (constructorForThisSource) {
        TValidator::CheckNotNull(BuildingFilters.FindPtr(mainMapInfo))->emplace_back(constructor);
    }
    Counters->OnFilterCacheMiss();
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->GetConclusion().GetErrorMessage());
        AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
        return;
    }
    LOCAL_LOG_TRACE("event", "filters_constructed")("sources", ev->Get()->GetConclusion().GetResult().size());
    for (auto&& [key, filter] : ev->Get()->ExtractResult()) {
        LOCAL_LOG_TRACE("event", "extract_constructed_filter")("range", key.DebugString());
        auto findWaiting = BuildingFilters.find(key);
        // AFL_VERIFY(findWaiting != BuildingFilters.end());
        if (findWaiting != BuildingFilters.end()) {
            for (const std::shared_ptr<TInternalFilterConstructor>& callback : findWaiting->second) {
                callback->AddFilter(key, std::move(filter));
            }
            BuildingFilters.erase(findWaiting);
        }

        FiltersCache.Insert(key, filter);
        if (UsePortionsCache) {
            PortionsCache[key.GetSourceId()].emplace(key);
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
