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
        AllocationGuard = std::move(allocationGuard);
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
        AFL_VERIFY(Callback);
        Callback->OnError(errorMessage);
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        AFL_VERIFY(Callback);
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
        AFL_VERIFY(Callback);
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
    , PKColumns(context.GetPKColumns())
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Intervals(MakeIntervalTree(portions))
    , Portions(MakePortionsIndex(Intervals))
    , FiltersCache(FILTER_CACHE_SIZE_CNT)
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    std::vector<TPortionInfo::TConstPtr> sourcesToFetch;
    const std::shared_ptr<TPortionInfo>& source = GetPortionVerified(ev->Get()->GetSourceId());
    {
        const auto collector = [&sourcesToFetch](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            sourcesToFetch.emplace_back(portion);
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

    auto constructor = std::make_shared<TInternalFilterConstructor>(ev);

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
        for (const std::shared_ptr<TInternalFilterConstructor>& callback : findWaiting->second) {
            callback->AddFilter(key, std::move(filter));
        }
        BuildingFilters.erase(findWaiting);

        FiltersCache.Insert(key, filter);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
