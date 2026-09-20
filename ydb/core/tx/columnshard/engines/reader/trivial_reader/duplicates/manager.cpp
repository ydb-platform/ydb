#include "executor.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/scanner.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

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
    for (const auto& portion : portions) {
        AFL_VERIFY(portionsStore.emplace(portion->GetPortionId(), portion).second);
    }
    return std::make_shared<TPortionStore>(std::move(portionsStore));
}

void TDuplicateManager::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    AbortAndPassAway("aborted by actor system");
}

void TDuplicateManager::AbortAndPassAway(const TString& error) {
    AbortionFlag->Inc();
    BordersFlowController.ClearInflightOnAbort();
    if (InflightExecutors) {
        Counters->OnFetchInflight(-static_cast<i64>(InflightExecutors));
        InflightExecutors = 0;
    }
    PendingExecutors.clear();
    for (auto& ev : PendingFilterRequests) {
        ev->Get()->GetSubscriber()->OnFailure(error);
    }
    PendingFilterRequests.clear();
    InflightFilterRequests = 0;
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

TDuplicateManager::TDuplicateManager(
    const TSpecialReadContext& context, const std::deque<std::shared_ptr<TPortionInfo>>& portions, const TDuration inflightTimeout)
    : TActor(&TDuplicateManager::StateMain)
    , LastSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetLastSchema())
    , PKColumns(context.GetPKColumns())
    , PKSchema(context.GetCommonContext()->GetReadMetadata()->GetIndexVersions().GetPrimaryKey())
    , Counters(context.GetCommonContext()->GetCounters().GetDuplicateFilteringCounters())
    , Portions(MakePortionsIndex(portions))
    , DataAccessorsManager(context.GetCommonContext()->GetDataAccessorsManager())
    , ColumnDataManager(context.GetCommonContext()->GetColumnDataManager())
    , BordersFlowController(
          std::make_shared<TMergeContext>(
              std::make_unique<NArrow::NMerger::TMergePartialStream>(PKSchema, nullptr,
                  context.GetCommonContext()->GetReadMetadata()->IsDescSorted(), IIndexInfo::GetSnapshotColumnNames(),
                  GetVersionBatch(context.GetCommonContext()->GetReadMetadata()->GetRequestSnapshot(), std::numeric_limits<ui64>::max()),
                  GetVersionBatch(TSnapshot::Max(), 0)), Counters, context.GetCommonContext()->GetReadMetadata()->IsDescSorted(), Portions,
              GetFetchingColumns()), portions, context.GetCommonContext()->GetReadMetadata(), Counters)
    , FiltersStore(context.GetCommonContext()->GetReadMetadata()->IsDescSorted(), Counters)
    , AbortionFlag(std::make_shared<TAtomicCounter>(0))
    , HangTracker(inflightTimeout)
{
}

bool TDuplicateManager::HasInflightFetchOrMerge() const {
    return InflightExecutors > 0 || BordersFlowController.IsMergeInflight();
}

void TDuplicateManager::OnProgress() {
    if (auto interval = HangTracker.OnProgress(TActivationContext::Monotonic())) {
        Schedule(*interval, new NActors::TEvents::TEvWakeup());
    }
}

void TDuplicateManager::HandleWakeup() {
    const auto result = HangTracker.OnWakeup(!AbortionFlag->Val() && HasInflightFetchOrMerge(), TActivationContext::Monotonic());
    if (result.TimedOut) {
        const TString error =
            TStringBuilder() << "duplicate filtering inflight timeout after " << HangTracker.GetTimeout()
                             << "; fetch_inflight=" << InflightExecutors << "; merge_inflight=" << BordersFlowController.IsMergeInflight()
                             << "; filter_requests_inflight=" << InflightFilterRequests << "; pending_executors=" << PendingExecutors.size()
                             << "; pending_filter_requests=" << PendingFilterRequests.size()
                             << "; borders_flow_controller=" << BordersFlowController.DebugString();
        YDB_LOG_ERROR("",
            {"component", "duplicates_manager"},
            {"event", "inflight_timeout"},
            {"timeout", HangTracker.GetTimeout().ToString()},
            {"fetch_inflight", InflightExecutors},
            {"merge_inflight", BordersFlowController.IsMergeInflight()},
            {"filter_requests_inflight", InflightFilterRequests},
            {"pending_executors", PendingExecutors.size()},
            {"pending_filter_requests", PendingFilterRequests.size()},
            {"borders_flow_controller", BordersFlowController.DebugString()});
        AbortAndPassAway(error);
        return;
    }
    if (result.RescheduleAfter) {
        Schedule(*result.RescheduleAfter, new NActors::TEvents::TEvWakeup());
    }
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    if (InflightFilterRequests < MaxInflightFilterRequests) {
        ++InflightFilterRequests;
        auto evCopy = ev;
        HandleFilterRequestImpl(evCopy);
    } else {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvRequestFilter")("type", "queued")(
            "portion_id", ev->Get()->GetPortionId())("pending_count", PendingFilterRequests.size());
        PendingFilterRequests.emplace_back(ev);
    }
}

void TDuplicateManager::HandleFilterRequestImpl(TEvRequestFilter::TPtr& ev) {
    TPortionInfo::TConstPtr mainPortion = Portions->GetPortionVerified(ev->Get()->GetPortionId());
    auto constructor = std::make_shared<TFilterAccumulator>(ev, Counters);
    if (BordersFlowController.ExtractExclusiveInterval(mainPortion->GetPortionId())) {
        auto filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, mainPortion->GetRecordsCount());
        constructor->AddFilter(std::move(filter));
        AFL_VERIFY(constructor->IsDone());
        Counters->OnRowsMerged(0, 0, mainPortion->GetRecordsCount());
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvRequestFilter")("type", "exclusive")(
            "info", constructor->DebugString());
        OnFilterRequestCompleted();
        return;
    }

    auto task =
        std::make_shared<TFilterSizeAllocation>(SelfId(), constructor, mainPortion->GetRecordsCount(), std::make_unique<TFilterBuildingGuard>());
    auto& filterGuard = task->GetRequestGuard();
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(filterGuard->GetMemoryProcessId(),
        filterGuard->GetMemoryScopeId(), filterGuard->GetMemoryGroupId(), { task }, (ui64)TFilterAccumulator::EFetchingStage::FILTERS);
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
        "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvRequestFilter")("type", "shared")(
        "info", constructor->DebugString());
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterRequestResourcesAllocated::TPtr& ev) {
    std::shared_ptr<TFilterAccumulator> constructor = ev->Get()->GetRequest();
    if (FiltersStore.NotifyReadyFilter(constructor)) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvFilterRequestResourcesAllocated")("type", "cached")(
            "info", constructor->DebugString());
        OnFilterRequestCompleted();
        return;
    }

    FiltersStore.AddWaitingPortion(constructor->GetRequest()->Get()->GetPortionId(), constructor);
    const std::shared_ptr<const TPortionInfo>& mainPortion = Portions->GetPortionVerified(constructor->GetRequest()->Get()->GetPortionId());

    TBordersIterator bordersIterator = BordersFlowController.Next(mainPortion);
    THashMap<ui64, TPortionInfo::TConstPtr> portionsToFetch;
    Counters->OnLeftBorders(-static_cast<i64>(bordersIterator.GetBorders().size()));
    for (const auto& border : bordersIterator.GetBorders()) {
        for (const auto& id : border.GetPortionIds()) {
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
    if (InflightExecutors < MaxInflightExecutors) {
        auto startSchedule = executor->ScheduleNext(std::move(columnFetchingRequest));
        if (startSchedule) {
            ++InflightExecutors;
            Counters->OnFetchInflight(1);
            OnProgress();
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")(
                "self", TActivationContext::AsActorContext().SelfID)("borders_flow_controller", BordersFlowController.DebugString())(
                "event", "TEvFilterRequestResourcesAllocated")("type", "inflight")("info", constructor->DebugString())("was_started", 1);
            return;
        }
        // No borders left for this request. Filter must be produced by an already running executor.
        // If nothing is running, the request would hang forever in WaitingPortions.
        if (InflightExecutors == 0 && PendingExecutors.empty()) {
            const ui64 portionId = constructor->GetRequest()->Get()->GetPortionId();
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "empty_borders_without_inflight")(
                "portion_id", portionId)("borders_flow_controller", BordersFlowController.DebugString());
            AFL_VERIFY(FiltersStore.AbortWaitingPortion(portionId, "no borders to build duplicate filter"));
            OnFilterRequestCompleted();
            return;
        }
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvFilterRequestResourcesAllocated")(
            "type", "waiting_other_inflight")("info", constructor->DebugString());
    } else {
        PendingExecutors.emplace_back(std::move(executor), std::move(columnFetchingRequest));
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvFilterRequestResourcesAllocated")("type", "queued")(
            "info", constructor->DebugString())("pending_count", PendingExecutors.size());
    }
}

void TDuplicateManager::Handle(const TEvBordersConstructionResult::TPtr& ev) {
    if (ev->Get()->Result.IsFail()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvBordersConstructionResult")(
            "error", ev->Get()->Result.GetErrorMessage());
        AbortAndPassAway(ev->Get()->Result.GetErrorMessage());
        return;
    }
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
        "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvBordersConstructionResult")("type", "finish")(
        "portions", ev->Get()->Context.GetBatch().GetPortionIds().size())("borders", ev->Get()->Context.GetBatch().GetBorders().size());

    OnProgress();
    BordersFlowController.Enqueue(ev);
}

void TDuplicateManager::Handle(const TEvMergeBordersResult::TPtr& ev) {
    auto& event = *ev->Get();
    if (event.Result.IsFail()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("self", TActivationContext::AsActorContext().SelfID)(
            "borders_flow_controller", BordersFlowController.DebugString())("event", "TEvMergeBordersResult")(
            "error", event.Result.GetErrorMessage());
        AbortAndPassAway(event.Result.GetErrorMessage());
        return;
    }
    OnProgress();
    if (!event.Context.GetExecutor()->ScheduleNext(event.Context.ExtractGlobalContext())) {
        Counters->OnFetchInflight(-1);
        AFL_VERIFY(InflightExecutors > 0);
        --InflightExecutors;
        TryStartPendingExecutor();
    }
    for (auto&& [portionId, filter] : event.ReadyFilters) {
        if (FiltersStore.AddReadyFilter(portionId, std::move(filter))) {
            OnFilterRequestCompleted();
        }
    }
    BordersFlowController.OnReadyMergeBorders();
}

void TDuplicateManager::TryStartPendingExecutor() {
    while (!PendingExecutors.empty() && InflightExecutors < MaxInflightExecutors) {
        auto pending = std::move(PendingExecutors.front());
        PendingExecutors.pop_front();
        auto startSchedule = pending.Executor->ScheduleNext(std::move(pending.Context));
        if (startSchedule) {
            ++InflightExecutors;
            Counters->OnFetchInflight(1);
            OnProgress();
        }
    }
}

void TDuplicateManager::OnFilterRequestCompleted() {
    AFL_VERIFY(InflightFilterRequests > 0);
    --InflightFilterRequests;
    TryStartPendingFilterRequest();
}

void TDuplicateManager::TryStartPendingFilterRequest() {
    while (!PendingFilterRequests.empty() && InflightFilterRequests < MaxInflightFilterRequests) {
        auto ev = std::move(PendingFilterRequests.front());
        PendingFilterRequests.pop_front();
        ++InflightFilterRequests;
        HandleFilterRequestImpl(ev);
    }
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
