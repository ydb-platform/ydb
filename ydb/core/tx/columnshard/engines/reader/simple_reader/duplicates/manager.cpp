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
}   // namespace

class TDuplicateManager::TPortionsSlice {
private:
    THashMap<ui64, TRowRange> RangeByPortion;
    TColumnDataSplitter::TBorder IntervalEnd;

public:
    TPortionsSlice(const TColumnDataSplitter::TBorder& end)
        : IntervalEnd(end)
    {
    }

    void AddRange(const ui64 portion, const TRowRange& range) {
        if (range.NumRows() == 0) {
            return;
        }
        AFL_VERIFY(RangeByPortion.emplace(portion, range).second);
    }

    const TRowRange* GetRangeOptional(const ui64 portion) const {
        return RangeByPortion.FindPtr(portion);
    }
    THashMap<ui64, TRowRange> GetRanges() const {
        return RangeByPortion;
    }
    const TColumnDataSplitter::TBorder& GetEnd() const {
        return IntervalEnd;
    }
};

std::vector<TDuplicateManager::TPortionsSlice> TDuplicateManager::FindIntervalBorders(
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion,
    const std::shared_ptr<TInternalFilterConstructor>& context) const {
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    for (const auto& [portionId, _] : dataByPortion) {
        const auto& portion = GetPortionVerified(portionId);
        borders.emplace(
            portionId, NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
    }
    const auto& mainSource = GetPortionVerified(context->GetRequest()->Get()->GetSourceId());
    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(mainSource->IndexKeyStart(), mainSource->IndexKeyEnd(), mainSource->IndexKeyStart().GetSchema()));

    std::vector<TDuplicateManager::TPortionsSlice> slices;
    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        slices.emplace_back(TPortionsSlice(splitter.GetIntervalFinish(i)));
    }
    for (const auto& [id, data] : dataByPortion) {
        auto intervals = splitter.SplitPortion(data);
        AFL_VERIFY(intervals.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            slices[i].AddRange(id, intervals[i]);
        }
    }
    return slices;
}

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
    , FiltersCache(FILTER_CACHE_SIZE_CNT)
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    auto constructor = std::make_shared<TInternalFilterConstructor>(ev);
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(constructor->GetMemoryProcessId(),
        constructor->GetMemoryScopeId(), constructor->GetMemoryGroupId(),
        { std::make_shared<TPortionIntersectionsAllocation>(SelfId(), constructor,
            ExpectedIntersectionCount * sizeof(TPortionInfo::TConstPtr)) }, (ui64)TInternalFilterConstructor::EFetchingStage::INTERSECTIONS);
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
    ExpectedIntersectionCount = sourcesToFetch.size();
    memoryGuard->Update(sourcesToFetch.size() * sizeof(TPortionInfo::TConstPtr));

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

    auto slices = FindIntervalBorders(dataByPortion, context);
    for (const auto& slice : slices) {
        BuildFilterForSlice(slice, context, allocationGuard, dataByPortion);
    }
}

void TDuplicateManager::BuildFilterForSlice(const TPortionsSlice& slice, const std::shared_ptr<TInternalFilterConstructor>& constructor,
    const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard,
    const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& dataByPortion) {
    const TSnapshot& maxVersion = constructor->GetRequest()->Get()->GetMaxVersion();
    const ui64 mainPortionId = constructor->GetRequest()->Get()->GetSourceId();

    auto findMainRange = slice.GetRangeOptional(mainPortionId);
    if (!findMainRange) {
        return;
    }

    TDuplicateMapInfo mainMapInfo(maxVersion, *findMainRange, mainPortionId);
    if (auto* findBuilding = BuildingFilters.FindPtr(mainMapInfo)) {
        AFL_VERIFY(findBuilding->empty())("existing", findBuilding->front()->DebugString())("new", constructor->DebugString())(
            "key", mainMapInfo.DebugString());
        findBuilding->emplace_back(constructor);
        return;
    }

    if (auto findCached = FiltersCache.Find(mainMapInfo); findCached != FiltersCache.End()) {
        constructor->AddFilter(findCached.Key(), findCached.Value());
        Counters->OnFilterCacheHit();
        return;
    }

    if (slice.GetRanges().size() == 1 && maxVersion >= GetPortionVerified(mainPortionId)->RecordSnapshotMax(maxVersion)) {
        NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainMapInfo.GetRows().NumRows());
        AFL_VERIFY(BuildingFilters.emplace(mainMapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>({constructor})).second);
        Send(SelfId(),
            new NPrivate::TEvFilterConstructionResult(THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>({ { mainMapInfo, filter } })));
        return;
    }

    NArrow::NMerger::TCursor maxVersionBatch = [&maxVersion]() {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, maxVersion, std::numeric_limits<ui64>::max());
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    }();

    const std::shared_ptr<TBuildDuplicateFilters> task = std::make_shared<TBuildDuplicateFilters>(
        PKSchema, maxVersionBatch, slice.GetEnd().GetKey(), slice.GetEnd().GetIsLast(), Counters, SelfId());
    for (const auto& [source, segment] : slice.GetRanges()) {
        const auto* columnData = dataByPortion.FindPtr(source);
        AFL_VERIFY(columnData)("source", source);
        TDuplicateMapInfo mapInfo(maxVersion, segment, source);
        task->AddSource(*columnData, allocationGuard, mapInfo);
        AFL_VERIFY(BuildingFilters.emplace(mapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>()).second);
    }
    NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
    TValidator::CheckNotNull(BuildingFilters.FindPtr(mainMapInfo))->emplace_back(constructor);
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
        AFL_VERIFY(findWaiting != BuildingFilters.end());
        AFL_VERIFY(findWaiting->second.size());
        Counters->OnFilterCacheHit(findWaiting->second.size() - 1);
        Counters->OnFilterCacheMiss();
        for (const std::shared_ptr<TInternalFilterConstructor>& callback : findWaiting->second) {
            callback->AddFilter(key, std::move(filter));
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
