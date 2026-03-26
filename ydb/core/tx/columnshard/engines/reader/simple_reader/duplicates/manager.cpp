#include "executor.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
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
    , Portions(MakePortionsIndex(portions))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
    , Merger(PKSchema, nullptr, false, IIndexInfo::GetSnapshotColumnNames(), GetVersionBatch(context.GetCommonContext()->GetReadMetadata()->GetRequestSnapshot(), std::numeric_limits<ui64>::max()), GetVersionBatch(TSnapshot::Max(), 0))
    , AbortionFlag(std::make_shared<TAtomicCounter>(0))
{
    for (const auto& portion : portions) {
        Borders[portion->IndexKeyStart()].Start.push_back(portion->GetPortionId());
        Borders[portion->IndexKeyEnd()].Finish.push_back(portion->GetPortionId());
    }
    BuildExclusivePortions();
    if (!Borders.empty()) {
        PreviousBorder = Borders.begin()->first;
        for (const auto& portionId : Borders.begin()->second.Start) {
            CurrentPortions.insert(portionId);
            ProcessedPortions.insert(portionId);
        }
    }
    Counters->OnLeftBorders(Borders.size());
}

bool TDuplicateManager::IsExclusiveInterval(const ui64 portionId) const {
    return ExclusivePortions.find(portionId) != ExclusivePortions.end();
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    TPortionInfo::TConstPtr mainPortion = Portions->GetPortionVerified(ev->Get()->GetPortionId());
    auto constructor = std::make_shared<TFilterAccumulator>(ev, Counters);
    if (IsExclusiveInterval(mainPortion->GetPortionId())) {
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainPortion->GetRecordsCount());
        constructor->AddFilter(std::move(std::move(filter)));
        AFL_VERIFY(constructor->IsDone());
        Counters->OnRowsMerged(0, 0, mainPortion->GetRecordsCount());
        return;
    }
    
    auto task = std::make_shared<TPortionIntersectionsAllocation>(SelfId(), constructor, mainPortion->GetRecordsCount(), std::make_unique<TFilterBuildingGuard>());
    auto& filterGuard = task->GetRequestGuard();
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(
        filterGuard->GetMemoryProcessId(),
        filterGuard->GetMemoryScopeId(), 
        filterGuard->GetMemoryGroupId(), { task },
        (ui64)TFilterAccumulator::EFetchingStage::FILTERS);
    return;
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TFilterAccumulator> constructor = ev->Get()->GetRequest();
    if (FiltersBuilder.NotifyReadyFilter(constructor)) {
        return;
    }
    
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> memoryGuard = ev->Get()->ExtractAllocationGuard();
    auto requestGuard = ev->Get()->ExtractRequestGuard();

    FiltersBuilder.AddWaitingPortion(constructor->GetRequest()->Get()->GetPortionId(), constructor);

    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());
    auto border = mainPortion->IndexKeyEnd();

    std::vector<TIntervalsIteratorBuilder> builders;
    builders.emplace_back();
    for (auto it = Borders.begin(); it != Borders.end() && it->first <= border;) {
        for (const auto& added: it->second.Start) {
            if (!ProcessedPortions.insert(added).second) {
                continue;
            }
            CurrentPortions.insert(added);
        }

        if (!CurrentPortions.empty()) {
            ui32 prevSize = WaitingBorders.size();
            ++WaitingBorders[*PreviousBorder];
            ++WaitingBorders[it->first];
            Counters->OnWaitingBorders(WaitingBorders.size() - prevSize);
            builders.back().AppendInterval(TIntervalBorder::First(std::make_shared<NArrow::NMerger::TSortableBatchPosition>(PreviousBorder->BuildSortablePosition()), 0), TIntervalBorder::First(std::make_shared<NArrow::NMerger::TSortableBatchPosition>(it->first.BuildSortablePosition()), 0), CurrentPortions);
            CurrentPortions.clear();
        }
        PreviousBorder = it->first;
        it = Borders.erase(it);
        Counters->OnLeftBorders(-1);
    }
    
    for (auto& builder: builders) {
        TIntervalsIterator intervalsIterator = builder.Build();
        THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
        for (const auto& id : intervalsIterator.GetNeededPortions()) {
            portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
        }
        
        if (portionsToFetch.empty()) {
            if (WaitingBorders.empty()) {
                Merger.PutControlPoint(border.BuildSortablePosition(), false);
                Merger.DrainToControlPoint(FiltersBuilder, true);
            }
            Counters->OnRequestCacheWaiting();
            return;
        }
        Counters->OnIntervalsPerRequest(intervalsIterator.GetIntervals().size());
        
        Counters->OnRequestCacheMiss();
        TBuildFilterContext columnFetchingRequest(SelfId(), AbortionFlag, constructor->GetRequest()->Get()->GetMaxVersion(),
            std::move(portionsToFetch), GetFetchingColumns(), PKSchema, LastSchema, ColumnDataManager, DataAccessorsManager, Counters,
            std::move(requestGuard), memoryGuard);
        std::shared_ptr<TBuildFilterTaskExecutor> executor = std::make_shared<TBuildFilterTaskExecutor>(std::move(intervalsIterator));
        AFL_VERIFY(executor->ScheduleNext(std::move(columnFetchingRequest)));
    }
}

void TDuplicateManager::Handle(const TEvIntervalConstructionResult::TPtr& ev) {
    if (ev->Get()->Result.IsFail()) {
        LOCAL_LOG_TRACE("event", "filter_construction_error")("error", ev->Get()->Result.GetErrorMessage());
        AbortAndPassAway(ev->Get()->Result.GetErrorMessage());
        return;
    }
    
    auto columnData = ev->Get()->Result->ExtractDataByPortion(GetFetchingColumns());
    for (const auto& [portionId, data] : columnData) {
        Merger.AddSource(data, nullptr, NArrow::NMerger::TIterationOrder::Forward(0), portionId);
        FiltersBuilder.AddSource(portionId, Portions->GetPortionVerified(portionId)->GetRecordsCount());
    }
    for (const auto& interval : ev->Get()->Context.GetIntervals()) {
        auto beginKey = NArrow::TSimpleRow{interval.GetBegin().GetKey()->MakeRecordBatch(), 0};
        auto endKey = NArrow::TSimpleRow{interval.GetEnd().GetKey()->MakeRecordBatch(), 0};
        auto beginIt = WaitingBorders.find(beginKey);
        AFL_VERIFY(beginIt != WaitingBorders.end() && beginIt->second > 0);
        --beginIt->second;
        auto endIt = WaitingBorders.find(endKey);
        AFL_VERIFY(endIt != WaitingBorders.end() && endIt->second > 0);
        --endIt->second;
    }
    while (!WaitingBorders.empty() && WaitingBorders.begin()->second == 0) {
        auto it = WaitingBorders.begin();
        Merger.PutControlPoint(it->first.BuildSortablePosition(), false);
        Merger.DrainToControlPoint(FiltersBuilder, true);
        WaitingBorders.erase(it);
        Counters->OnWaitingBorders(-1);
    }
    Counters->OnRowsMerged(FiltersBuilder.GetRowsAdded() - PrevRowsAdded, FiltersBuilder.GetRowsSkipped() - PrevRowsSkipped, 0);
    PrevRowsAdded = FiltersBuilder.GetRowsAdded();
    PrevRowsSkipped = FiltersBuilder.GetRowsSkipped();
    
    if (Borders.empty() && WaitingBorders.empty()) {
        Merger.DrainAll(FiltersBuilder);
    }
    
    if (WaitingBorders.empty() && PreviousBorder) {
        Merger.PutControlPoint(PreviousBorder->BuildSortablePosition(), false);
        Merger.DrainToControlPoint(FiltersBuilder, true);
    }
    ev->Get()->Context.GetExecutor()->ScheduleNext(ev->Get()->Context.ExtractGlobalContext());
}

void TDuplicateManager::BuildExclusivePortions() {
    size_t openIntervals = 0;
    
    for (auto it = Borders.begin(); it != Borders.end();) {
        auto currentIt = it;
        ++it;
        
        if (openIntervals == 0 && ((currentIt->second.Start.size() == 1 && currentIt->second.Finish.size() == 1) || (it == Borders.end() || (it->second.Finish.size() == 1 && it->second.Start.empty())))) {
            if (currentIt->second.Start.size() == 1) {
                ExclusivePortions.insert(currentIt->second.Start.front());
            }
        }

        openIntervals += currentIt->second.Start.size();
        openIntervals -= currentIt->second.Finish.size();
    }
    
    for (auto it = Borders.begin(); it != Borders.end();) {
        if (it->second.Start.size() == 1 && it->second.Finish.size() == 0 && ExclusivePortions.count(it->second.Start.front())) {
            it = Borders.erase(it);
            continue;
        }
        
        if (it->second.Start.size() == 0 && it->second.Finish.size() == 1 && ExclusivePortions.count(it->second.Finish.front())) {
            it = Borders.erase(it);
            continue;
        }
        
        ++it;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
