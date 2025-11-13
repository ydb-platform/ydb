#include "executor.h"
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

TIntervalsInterator TDuplicateManager::StartIntervalProcessing(
    const THashSet<ui64>& intersectingPortions, const std::shared_ptr<TFilterAccumulator>& constructor) {
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetSourceId());
    THashMap<ui64, TSortableBorders> materializedBorders;
    for (const auto& portionId : intersectingPortions) {
        materializedBorders.emplace(portionId, GetBorders(portionId));
    }
    TColumnDataSplitter splitter(materializedBorders);
    LOCAL_LOG_TRACE("event", "split_portion")
    ("source", constructor->GetRequest()->Get()->GetSourceId())("splitter", splitter.DebugString())(
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
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetSourceId());
    {
        const auto collector = [&intersectingPortions](
                                   const TPortionIntervalTree::TRange& /*interval*/, const std::shared_ptr<TPortionInfo>& portion) {
            AFL_VERIFY(intersectingPortions.insert(portion->GetPortionId()).second);
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

    TIntervalsInterator intervalsIterator = StartIntervalProcessing(intersectingPortions, constructor);

    if (!intervalsIterator.IsDone()) {
        THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
        for (const auto& id : intervalsIterator.GetNeededPortions()) {
            portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
        }

        AFL_VERIFY(!constructor->IsDone());
        TBuildFilterContext columnFetchingRequest(SelfId(), constructor->GetRequest()->Get()->GetAbortionFlag(),
            constructor->GetRequest()->Get()->GetMaxVersion(), std::move(portionsToFetch), GetFetchingColumns(), PKSchema, LastSchema,
            ColumnDataManager, DataAccessorsManager, Counters, std::move(requestGuard), memoryGuard);
        memoryGuard->Update(columnFetchingRequest.GetDataSize());

        for (const auto& interval : intervalsIterator.GetIntervals()) {
            auto findInFlight = IntervalsInFlight.FindPtr(interval.MakeView());
            AFL_VERIFY(findInFlight);
            findInFlight->SetJob(columnFetchingRequest.GetStatus());
        }

        std::shared_ptr<TBuildFilterTaskExecutor> executor = std::make_shared<TBuildFilterTaskExecutor>(std::move(intervalsIterator));
        AFL_VERIFY(executor->ScheduleNext(std::move(columnFetchingRequest)));
    }

    ValidateInFlightProgress();
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
            findInterval->second.OnFilterReady(mapInfo.GetSourceId(), filter);
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
