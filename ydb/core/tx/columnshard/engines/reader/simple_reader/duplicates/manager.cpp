#include "executor.h"
#include "manager.h"
#include "pk_fetcher.h"
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

class TPKFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    TBuildFilterContext Context;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(Context.GetOwner(),
                new NPrivate::TEvFilterConstructionResult(
                    TConclusionStatus::Fail(errorAddresses.GetErrorMessage()), Context.MakeResultInFlightGuard()));
            return;
        }

        AFL_VERIFY(AllocationGuard);
        
        // Create task to extract PK keys
        const std::shared_ptr<TFetchPKKeysTask> task =
            std::make_shared<TFetchPKKeysTask>(std::move(Context), std::move(objectAddresses), AllocationGuard);
        NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(std::static_pointer_cast<NConveyor::ITask>(task));
    }

    virtual bool DoIsAborted() const override {
        return Context.GetAbortionFlag() && Context.GetAbortionFlag()->Val();
    }

public:
    TPKFetchingCallback(TBuildFilterContext&& context, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Context(std::move(context))
        , AllocationGuard(allocationGuard)
    {
        AFL_VERIFY(allocationGuard);
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Context.GetOwner());
        TActorContext::AsActorContext().Send(
            Context.GetOwner(), new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(errorMessage),
                                                       Context.MakeResultInFlightGuard()));
    }
};

class TPKDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterContext Context;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Context.GetOwner(),
            new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage),
                Context.MakeResultInFlightGuard()));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        // For PK fetching, we only need to load PK columns from the main portion
        AFL_VERIFY(Context.GetUseKeyBasedDedup());
        const ui64 mainPortionId = Context.GetMainPortionId();
        auto mainPortion = Context.GetPortion(mainPortionId);
        
        THashSet<TPortionAddress> portionAddresses;
        portionAddresses.insert(mainPortion->GetAddress());
        
        // Get only PK column IDs from the snapshot schema
        auto snapshotSchema = Context.GetSnapshotSchema();
        auto pkSchema = Context.GetPKSchema();
        std::set<ui32> pkColumnIds;
        for (int i = 0; i < pkSchema->num_fields(); ++i) {
            auto fieldName = pkSchema->field(i)->name();
            auto columnId = snapshotSchema->GetColumnIdVerified(fieldName);
            pkColumnIds.insert(columnId);
        }
        
        auto columnDataManager = Context.GetColumnDataManager();
        columnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, portionAddresses, std::move(pkColumnIds),
            std::make_shared<TPKFetchingCallback>(std::move(Context), guard));
        return true;
    }

public:
    TPKDataAllocation(TBuildFilterContext&& context, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Context(std::move(context))
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

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions)
    : TActor(&TDuplicateManager::StateMain)
    , LastSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema())
    , PKColumns(context.GetPKColumns())
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Intervals(MakeIntervalTree(portions))
    , Portions(MakePortionsIndex(Intervals))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
    , KeyBasedDedupThreshold(AppDataVerified().ColumnShardConfig.GetKeyBasedDedupThreshold())
    , FiltersCache(FILTER_CACHE_SIZE)
    , MaterializedBordersCache(BORDER_CACHE_SIZE_COUNT)
    , AbortionFlag(std::make_shared<TAtomicCounter>(0))
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
    TPortionInfo::TConstPtr mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());
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

TIntervalsIterator TDuplicateManager::StartIntervalProcessing(
    const THashSet<ui64>& intersectingPortions, const std::shared_ptr<TFilterAccumulator>& constructor, const bool useKeyBasedDedup) {
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());
    
    if (useKeyBasedDedup) {
        // For key-based deduplication, create a single interval covering the entire portion
        // The actual deduplication will be done by comparing PK keys during merge
        LOCAL_LOG_TRACE("event", "key_based_dedup")
        ("source", constructor->GetRequest()->Get()->GetPortionId())("intersection_portions", intersectingPortions.size());
        
        constructor->SetIntervalsCount(1);
        
        // Build list of portions that intersect with main portion by keys
        // This is already done in Handle() via Intervals.EachIntersection
        // intersectingPortions contains all portions that intersect with mainPortion
        
        // Create a single interval covering the entire main portion
        TIntervalBorder begin = TIntervalBorder::First(
            std::make_shared<NArrow::NMerger::TSortableBatchPosition>(mainPortion->IndexKeyStart().BuildSortablePosition()),
            mainPortion->GetPortionId());
        TIntervalBorder end = TIntervalBorder::Last(
            std::make_shared<NArrow::NMerger::TSortableBatchPosition>(mainPortion->IndexKeyEnd().BuildSortablePosition()),
            mainPortion->GetPortionId());
        
        TIntervalBordersView intervalView(begin.MakeView(), end.MakeView());
        
        // Check cache
        if (auto findCached = FiltersCache.Find(
                TDuplicateMapInfo(constructor->GetRequest()->Get()->GetMaxVersion(), intervalView, mainPortion->GetPortionId()));
            findCached != FiltersCache.End()) {
            constructor->AddFilter(0, findCached.Value());
            Counters->OnFilterCacheHit();
            return TIntervalsIteratorBuilder::BuildKeyBasedIterator(intersectingPortions, mainPortion->GetPortionId());
        }
        
        // For key-based deduplication, we don't add intervals to IntervalsInFlight
        // because we don't build filters in the traditional way
        // The filter will be built after PK keys are fetched
        Counters->OnFilterCacheMiss();
        
        return TIntervalsIteratorBuilder::BuildKeyBasedIterator(intersectingPortions, mainPortion->GetPortionId());
    }
    
    // Original boundary-based deduplication
    THashMap<ui64, TSortableBorders> materializedBorders;
    for (const auto& portionId : intersectingPortions) {
        materializedBorders.emplace(portionId, GetBorders(portionId));
    }
    TColumnDataSplitter splitter(materializedBorders);
    LOCAL_LOG_TRACE("event", "split_portion")
    ("source", constructor->GetRequest()->Get()->GetPortionId())("splitter", splitter.DebugString())(
        "intersection_portions", intersectingPortions.size());
    THashMap<ui32, NArrow::TColumnFilter> readyFilters;
    std::vector<ui32> intervalsToBuild;
    {
        ui64 nextIntervalIdx = 0;
        auto scheduleInterval = [&](const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& /*portions*/) {
            ++nextIntervalIdx;
            TIntervalBordersView intervalView(begin.MakeView(), end.MakeView());
            if (auto findCached = FiltersCache.Find(
                    TDuplicateMapInfo(constructor->GetRequest()->Get()->GetMaxVersion(), intervalView, mainPortion->GetPortionId()));
                findCached != FiltersCache.End()) {
                AFL_VERIFY(readyFilters.emplace(nextIntervalIdx - 1, findCached.Value()).second);
                Counters->OnFilterCacheHit();
                return true;
            }
            auto [inFlight, emplaced] = IntervalsInFlight.emplace(intervalView, TIntervalInFlightInfo());
            inFlight->second.AddSubscriber(mainPortion->GetPortionId(), TIntervalFilterCallback(nextIntervalIdx - 1, constructor));
            if (emplaced) {
                intervalsToBuild.emplace_back(nextIntervalIdx - 1);
                Counters->OnFilterCacheMiss();
            } else {
                Counters->OnFilterCacheHit();
            }
            return true;
        };
        splitter.ForEachIntersectingInterval(std::move(scheduleInterval), mainPortion->GetPortionId());
        constructor->SetIntervalsCount(nextIntervalIdx);
    }
    for (auto&& [idx, filter] : std::move(readyFilters)) {
        constructor->AddFilter(idx, std::move(filter));
    }
    return TIntervalsIteratorBuilder::BuildFromSplitter(splitter, intervalsToBuild, mainPortion->GetPortionId());
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TFilterAccumulator> constructor = ev->Get()->GetRequest();
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> memoryGuard = ev->Get()->ExtractAllocationGuard();
    auto requestGuard = ev->Get()->ExtractRequestGuard();

    THashSet<ui64> intersectingPortions;
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());
    {
        const auto collector = [&intersectingPortions](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            AFL_VERIFY(intersectingPortions.insert(portion->GetPortionId()).second);
            return true;
        };
        Intervals.EachIntersection(
            TPortionIntervalTree::TRange(mainPortion->IndexKeyStart(), true, mainPortion->IndexKeyEnd(), true), collector);
    }


    // Use key-based deduplication when there are too many intersections
    const bool useKeyBasedDedup = intersectingPortions.size() > KeyBasedDedupThreshold;
    
    TIntervalsIterator intervalsIterator = StartIntervalProcessing(intersectingPortions, constructor, useKeyBasedDedup);
    ExpectedIntersectionCount = intersectingPortions.size();
    if (!useKeyBasedDedup) {
        Counters->OnFilterRequest(intersectingPortions.size());

        LOCAL_LOG_TRACE("event", "request_filter")
        ("source", constructor->GetRequest()->Get()->GetPortionId())("intersecting_portions", intersectingPortions.size());
        AFL_VERIFY(intersectingPortions.size());
    }

    if (!intervalsIterator.IsDone()) {
        if (useKeyBasedDedup) {
            // For key-based deduplication, first fetch PK keys from main portion
            THashMap<ui64, TPortionInfo::TConstPtr> mainPortionOnly;
            mainPortionOnly.emplace(mainPortion->GetPortionId(), mainPortion);
            
            // Build PK-only columns to fetch
            // Use correct column IDs from snapshot schema
            std::map<ui32, std::shared_ptr<arrow::Field>> pkColumns;
            for (int i = 0; i < PKSchema->num_fields(); ++i) {
                auto fieldName = PKSchema->field(i)->name();
                auto columnId = LastSchema->GetColumnIdVerified(fieldName);
                pkColumns.emplace(columnId, PKSchema->field(i));
            }
            
            // Create a new request guard for PK fetching
            auto pkRequestGuard = std::make_unique<TFilterBuildingGuard>();
            
            TBuildFilterContext pkFetchRequest(SelfId(), AbortionFlag, constructor->GetRequest()->Get()->GetMaxVersion(),
                std::move(mainPortionOnly), pkColumns, PKSchema, LastSchema, ColumnDataManager, DataAccessorsManager, Counters,
                std::move(pkRequestGuard), memoryGuard, useKeyBasedDedup, mainPortion->GetPortionId(), nullptr, constructor);
            memoryGuard->Update(pkFetchRequest.GetDataSize());
            
            // Schedule PK fetching task using TPKDataAllocation
            // Estimate memory for PK data: assume 100 bytes per row for PK
            const ui64 pkDataSize = mainPortion->GetRecordsCount() * 100;
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(
                pkFetchRequest.GetRequestGuard()->GetMemoryProcessId(),
                pkFetchRequest.GetRequestGuard()->GetMemoryScopeId(),
                pkFetchRequest.GetRequestGuard()->GetMemoryGroupId(),
                { std::make_shared<TPKDataAllocation>(std::move(pkFetchRequest), pkDataSize) },
                (ui64)TFilterAccumulator::EFetchingStage::PK_ONLY);
        } else {
            // Regular boundary-based deduplication
            THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
            for (const auto& id : intervalsIterator.GetNeededPortions()) {
                portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
            }

            AFL_VERIFY(!constructor->IsDone());
            TBuildFilterContext columnFetchingRequest(SelfId(), AbortionFlag, constructor->GetRequest()->Get()->GetMaxVersion(),
                std::move(portionsToFetch), GetFetchingColumns(), PKSchema, LastSchema, ColumnDataManager, DataAccessorsManager, Counters,
                std::move(requestGuard), memoryGuard, useKeyBasedDedup, mainPortion->GetPortionId(), nullptr, constructor);
            memoryGuard->Update(columnFetchingRequest.GetDataSize());

            for (const auto& interval : intervalsIterator.GetIntervals()) {
                auto findInFlight = IntervalsInFlight.FindPtr(interval.MakeView());
                AFL_VERIFY(findInFlight);
                findInFlight->SetJob(columnFetchingRequest.GetStatus());
            }

            std::shared_ptr<TBuildFilterTaskExecutor> executor = std::make_shared<TBuildFilterTaskExecutor>(std::move(intervalsIterator));
            AFL_VERIFY(executor->ScheduleNext(std::move(columnFetchingRequest)));
        }
    }

    ValidateInFlightProgress();
}
void TDuplicateManager::Handle(const NPrivate::TEvPKKeysFetched::TPtr& ev) {
    // PK keys have been fetched from main portion
    // Now find portions that intersect with these specific PK keys
    TBuildFilterContext context = ev->Get()->ExtractContext();
    
    const auto& pkKeys = context.GetMainPortionPKKeys();
    if (!pkKeys || pkKeys->empty()) {
        // No PK keys fetched, send error
        TActivationContext::AsActorContext().Send(context.GetOwner(),
            new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail("No PK keys fetched"),
                context.MakeResultInFlightGuard()));
        return;
    }
    
    // Find portions that intersect with each PK key
    THashSet<ui64> intersectingPortions;
    for (const auto& pkKey : *pkKeys) {
        const auto collector = [&intersectingPortions](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            intersectingPortions.insert(portion->GetPortionId());
            return true;
        };
        Intervals.EachIntersection(TPortionIntervalTree::TRange(pkKey, true, pkKey, true), collector);
    }
    
    Counters->OnFilterRequest(intersectingPortions.size());
    ExpectedIntersectionCount = intersectingPortions.size();
    
    LOCAL_LOG_TRACE("event", "pk_keys_fetched")
    ("main_portion", context.GetMainPortionId())("pk_keys_count", pkKeys->size())("intersecting_portions", intersectingPortions.size());
    
    // Get the filter accumulator
    auto constructor = context.GetFilterAccumulator();
    if (!constructor) {
        TActivationContext::AsActorContext().Send(context.GetOwner(),
            new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail("No filter accumulator"),
                context.MakeResultInFlightGuard()));
        return;
    }
    
    // If no intersecting portions found, all rows are accepted
    if (intersectingPortions.empty()) {
        auto mainPortion = Portions->GetPortionVerified(context.GetMainPortionId());
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainPortion->GetRecordsCount());
        constructor->ClearFilters();
        constructor->SetIntervalsCount(1);
        constructor->AddFilter(0, std::move(filter));
        Counters->OnRowsMerged(0, 0, mainPortion->GetRecordsCount());
        return;
    }
    
    // Build portions to fetch
    THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
    for (const auto& id : intersectingPortions) {
        portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
    }
    
    // Create a new request guard for data fetching
    auto dataRequestGuard = std::make_unique<TFilterBuildingGuard>();
    
    // Create context for data fetching with only intersecting portions
    TBuildFilterContext dataFetchRequest(SelfId(), AbortionFlag, constructor->GetRequest()->Get()->GetMaxVersion(),
        std::move(portionsToFetch), GetFetchingColumns(), PKSchema, LastSchema, ColumnDataManager, DataAccessorsManager, Counters,
        std::move(dataRequestGuard), context.GetSelfMemory(), true, context.GetMainPortionId(), pkKeys, constructor);
    
    // Create intervals iterator for key-based deduplication
    TIntervalsIterator intervalsIterator = TIntervalsIteratorBuilder::BuildKeyBasedIterator(intersectingPortions, context.GetMainPortionId());
    
    // Create executor and schedule next task
    std::shared_ptr<TBuildFilterTaskExecutor> executor = std::make_shared<TBuildFilterTaskExecutor>(std::move(intervalsIterator));
    AFL_VERIFY(executor->ScheduleNext(std::move(dataFetchRequest)));
}


void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr& ev) {
    if (ev->Get()->GetConclusion().IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->GetConclusion().GetErrorMessage());
        AbortAndPassAway(ev->Get()->GetConclusion().GetErrorMessage());
        return;
    }
    LOCAL_LOG_TRACE("event", "filters_constructed")("filters", ev->Get()->GetConclusion().GetResult().size());
    AFL_VERIFY(ev->Get()->GetConclusion().GetResult().size());
    for (auto&& [mapInfo, filter] : ev->Get()->ExtractResult()) {
        if (auto findInterval = IntervalsInFlight.find(mapInfo.GetInterval()); findInterval != IntervalsInFlight.end()) {
            findInterval->second.OnFilterReady(mapInfo.GetPortionId(), filter);
            if (findInterval->second.IsDone()) {
                IntervalsInFlight.erase(findInterval);
            }
        }
        FiltersCache.Insert(mapInfo, filter);
        LOCAL_LOG_TRACE("event", "extract_constructed_filter")("range", mapInfo.DebugString());
    }

    ValidateInFlightProgress();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
