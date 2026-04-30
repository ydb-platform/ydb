#include "executor.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

namespace {

class TFilterSizeAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TActorId Owner;
    std::shared_ptr<TFilterAccumulator> Request;
    YDB_READONLY_DEF(std::unique_ptr<TFilterBuildingGuard>, RequestGuard);

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Request->Abort(TStringBuilder() << "cannot allocate memory (filter size allocation): " << errorMessage);
    }

    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        TActorContext::AsActorContext().Send(Owner, new NPrivate::TEvFilterRequestResourcesAllocated(Request, guard, std::move(RequestGuard)));
        return true;
    }

public:
    TFilterSizeAllocation(const TActorId& owner, const std::shared_ptr<TFilterAccumulator>& request, const ui64 mem,
        std::unique_ptr<TFilterBuildingGuard>&& requestGuard)
        : NGroupedMemoryManager::IAllocation(mem)
        , Owner(owner)
        , Request(request)
        , RequestGuard(std::move(requestGuard))
    {
    }
};

}   // namespace

NArrow::NMerger::TCursor TDuplicateManager::GetVersionBatch(const TSnapshot& snapshot, const ui64 writeId) {
    NArrow::TGeneralContainer batch(1);
    IIndexInfo::AddSnapshotColumns(batch, snapshot, writeId);
    return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
}

std::shared_ptr<TPortionStore> TDuplicateManager::MakePortionsIndex(const std::deque<std::shared_ptr<TPortionInfo>>& portions) {
    THashMap<ui64, TPortionInfo::TConstPtr> portionsStore;
    for (const auto& portion: portions) {
        AFL_VERIFY(portionsStore.emplace(portion->GetPortionId(), portion).second);
    }
    return std::make_shared<TPortionStore>(std::move(portionsStore));
}

void TDuplicateManager::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    AbortAndPassAway("aborted by actor system");
}

void TDuplicateManager::AbortAndPassAway(const TString& error) {
    AbortionFlag->Inc();
    FiltersStore.Abort(error);
    PassAway();
}

std::map<ui32, std::shared_ptr<arrow::Field>> TDuplicateManager::GetFetchingColumns() const {
    std::map<ui32, std::shared_ptr<arrow::Field>> fieldsByColumn;
    for (const auto& columnId : PKColumns->GetColumnIds()) {
        fieldsByColumn.emplace(columnId, PKColumns->GetFilteredSchemaVerified().GetFieldByColumnIdVerified(columnId));
    }
    for (const auto& columnId : TIndexInfo::GetSnapshotColumnIds()) {
        fieldsByColumn.emplace(columnId, IIndexInfo::GetColumnFieldVerified(columnId));
    }
    return fieldsByColumn;
}

#define LOCAL_LOG_TRACE \
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)("borders_flow_controller", BordersFlowController.DebugString())

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions)
    : TActor(&TDuplicateManager::StateMain)
    , LastSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema())
    , PKColumns(context.GetPKColumns())
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Portions(MakePortionsIndex(portions))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
    , BordersFlowController(std::make_shared<TMergeContext>(
        std::make_unique<NArrow::NMerger::TMergePartialStream>(PKSchema, nullptr, context.GetCommonContext()->GetReadMetadata()->IsDescSorted(), IIndexInfo::GetSnapshotColumnNames(), GetVersionBatch(context.GetCommonContext()->GetReadMetadata()->GetRequestSnapshot(), std::numeric_limits<ui64>::max()), GetVersionBatch(TSnapshot::Max(), 0)),
        Counters,
        context.GetCommonContext()->GetReadMetadata()->IsDescSorted(),
        Portions,
        GetFetchingColumns()
      ), portions, context.GetCommonContext()->GetReadMetadata(), Counters)
    , FiltersStore(context.GetCommonContext()->GetReadMetadata()->IsDescSorted(), Counters)
    , AbortionFlag(std::make_shared<TAtomicCounter>(0))
{
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    TPortionInfo::TConstPtr mainPortion = Portions->GetPortionVerified(ev->Get()->GetPortionId());
    auto constructor = std::make_shared<TFilterAccumulator>(ev, Counters);
    if (BordersFlowController.ExtractExclusiveInterval(mainPortion->GetPortionId())) {
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainPortion->GetRecordsCount());
        constructor->AddFilter(std::move(filter));
        AFL_VERIFY(constructor->IsDone());
        Counters->OnRowsMerged(0, 0, mainPortion->GetRecordsCount());
        LOCAL_LOG_TRACE("event", "TEvRequestFilter")
            ("type", "exclusive")
            ("info", constructor->DebugString());
        return;
    }

    auto task = std::make_shared<TFilterSizeAllocation>(SelfId(), constructor, mainPortion->GetRecordsCount(), std::make_unique<TFilterBuildingGuard>());
    auto& filterGuard = task->GetRequestGuard();
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(
        filterGuard->GetMemoryProcessId(),
        filterGuard->GetMemoryScopeId(),
        filterGuard->GetMemoryGroupId(), { task },
        (ui64)TFilterAccumulator::EFetchingStage::FILTERS);
    LOCAL_LOG_TRACE("event", "TEvRequestFilter")
        ("type", "shared")
        ("info", constructor->DebugString());
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TFilterAccumulator> constructor = ev->Get()->GetRequest();
    if (FiltersStore.NotifyReadyFilter(constructor)) {
        LOCAL_LOG_TRACE("event", "TEvFilterRequestResourcesAllocated")
            ("type", "cached")
            ("info", constructor->DebugString());
        return;
    }

    FiltersStore.AddWaitingPortion(constructor->GetRequest()->Get()->GetPortionId(), constructor);
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());

    TBordersIterator bordersIterator = BordersFlowController.Next(mainPortion);
    THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
    Counters->OnLeftBorders(-static_cast<i64>(bordersIterator.GetBorders().size()));
    for (const auto& border : bordersIterator.GetBorders()) {
        for (const auto& id: border.GetPortionIds()) {
            portionsToFetch.emplace(id, Portions->GetPortionVerified(id));
        }
    }
    Counters->OnBordersPerRequest(bordersIterator.GetBorders().size());
    Counters->OnRequestCacheMiss();

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> memoryGuard = ev->Get()->ExtractAllocationGuard();
    auto requestGuard = ev->Get()->ExtractRequestGuard();
    TBuildFilterContext columnFetchingRequest(SelfId(), AbortionFlag, constructor->GetRequest()->Get()->GetMaxVersion(),
        std::move(portionsToFetch), GetFetchingColumns(), PKSchema, LastSchema, ColumnDataManager, DataAccessorsManager, Counters,
        std::move(requestGuard), memoryGuard);
    std::shared_ptr<TBuildFilterTaskExecutor> executor = std::make_shared<TBuildFilterTaskExecutor>(std::move(bordersIterator));
    auto startSchedule = executor->ScheduleNext(std::move(columnFetchingRequest));
    LOCAL_LOG_TRACE("event", "TEvFilterRequestResourcesAllocated")
        ("type", "inflight")
        ("info", constructor->DebugString())
        ("was_started", startSchedule);
}

void TDuplicateManager::Handle(const TEvBordersConstructionResult::TPtr& ev) {
    if (ev->Get()->Result.IsFail()) {
        LOCAL_LOG_TRACE("event", "TEvBordersConstructionResult")("error", ev->Get()->Result.GetErrorMessage());
        AbortAndPassAway(ev->Get()->Result.GetErrorMessage());
        return;
    }
    LOCAL_LOG_TRACE("event", "TEvBordersConstructionResult")
        ("type", "finish")
        ("portions", ev->Get()->Context.GetBatch().GetPortionIds().size())
        ("borders", ev->Get()->Context.GetBatch().GetBorders().size());

    BordersFlowController.Enqueue(ev);
}

void TDuplicateManager::Handle(const TEvMergeBordersResult::TPtr& ev) {
    auto& event = *ev->Get();
    if (event.Result.IsFail()) {
        LOCAL_LOG_TRACE("event", "TEvMergeBordersResult")("error", event.Result.GetErrorMessage());
        AbortAndPassAway(event.Result.GetErrorMessage());
        return;
    }
    event.Context.GetExecutor()->ScheduleNext(event.Context.ExtractGlobalContext());
    for (auto&& [portionId, filter] : event.ReadyFilters) {
        FiltersStore.AddReadyFilter(portionId, std::move(filter));
    }
    BordersFlowController.OnReadyMergeBorders();
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
