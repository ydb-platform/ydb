#include "executor.h"
#include "private_events.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {

class TColumnFetchingCallback: public ::NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TColumnDataCachePolicy> {
private:
    using TAddress = NGeneralCache::TGlobalColumnAddress;

    TBuildFilterTaskContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoOnResultReady(THashMap<TAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& objectAddresses,
        THashSet<TAddress>&& /*removedAddresses*/,
        ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TColumnDataCachePolicy>&& errorAddresses) override {
        if (errorAddresses.HasErrors()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvBordersConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(errorAddresses.GetErrorMessage())));
            return;
        }

        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvBordersConstructionResult>(std::move(Request), std::move(objectAddresses), std::move(AllocationGuard)));
    }

    virtual bool DoIsAborted() const override {
        return Request.GetGlobalContext().GetAbortionFlag() && Request.GetGlobalContext().GetAbortionFlag()->Val();
    }

public:
    TColumnFetchingCallback(TBuildFilterTaskContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Request(std::move(request))
        , AllocationGuard(allocationGuard)
    {
        AFL_VERIFY(allocationGuard);
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Request.GetGlobalContext().GetOwner());
        TActorContext::AsActorContext().Send(
            Request.GetGlobalContext().GetOwner(), std::make_unique<TEvBordersConstructionResult>(std::move(Request), TConclusionStatus::Fail(errorMessage)));
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterTaskContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvBordersConstructionResult>(std::move(Request), TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage)));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        THashSet<TPortionAddress> portionAddresses;
        for (const ui64 portionId : Request.GetBatch().GetPortionIds()) {
            AFL_VERIFY(portionAddresses.emplace(Request.GetGlobalContext().GetPortion(portionId)->GetAddress()).second);
        }
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("component", "duplicates_manager")
            ("type", "ask_column_data")
            ("addresses_count", portionAddresses.size());
        auto columnDataManager = Request.GetGlobalContext().GetColumnDataManager();
        auto columns = Request.GetGlobalContext().GetFetchingColumnIds();
        columnDataManager->AskColumnData(NBlobOperations::EConsumer::DUPLICATE_FILTERING, portionAddresses, std::move(columns),
            std::make_shared<TColumnFetchingCallback>(std::move(Request), guard));
        return true;
    }

public:
    TColumnDataAllocation(TBuildFilterTaskContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

class TColumnDataAccessorFetching: public IDataAccessorRequestsSubscriber {
private:
    TBuildFilterTaskContext Request;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsMemoryGuard;

private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        if (result.HasErrors()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvBordersConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(result.GetErrorMessage())));
            return;
        }

        if (result.HasRemovedData()) {
            TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
                std::make_unique<TEvBordersConstructionResult>(std::move(Request),
                    TConclusionStatus::Fail(TStringBuilder{} << "Has removed accessors data, count " << result.GetRemovedData().size())));
            return;
        }

        ui64 mem = 0;
        for (const auto& accessor : result.ExtractPortionsVector()) {
            mem += accessor->GetColumnRawBytes(Request.GetGlobalContext().GetFetchingColumnIds(), false);
        }

        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(
            Request.GetGlobalContext().GetRequestGuard()->GetMemoryProcessId(), Request.GetGlobalContext().GetRequestGuard()->GetMemoryScopeId(),
            Request.GetGlobalContext().GetRequestGuard()->GetMemoryGroupId(),
            { std::make_shared<TColumnDataAllocation>(std::move(Request), mem) }, (ui64)TFilterAccumulator::EFetchingStage::COLUMN_DATA);
    }
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Request.GetGlobalContext().GetAbortionFlag();
    }

public:
    TColumnDataAccessorFetching(
        TBuildFilterTaskContext&& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& accessorsMemoryGuard)
        : Request(std::move(request))
        , AccessorsMemoryGuard(accessorsMemoryGuard)
    {
    }

    static ui64 GetRequiredMemory(const THashSet<ui64>& portions, const TBuildFilterContext& context) {
        TDataAccessorsRequest dataAccessorsRequest(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const ui64 portionId : portions) {
            dataAccessorsRequest.AddPortion(context.GetPortion(portionId));
        }
        return dataAccessorsRequest.PredictAccessorsMemory(context.GetSnapshotSchema());
    }
};

class TDataAccessorAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterTaskContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            std::make_unique<TEvBordersConstructionResult>(std::move(Request), TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage)));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const ui64 portionId : Request.GetBatch().GetPortionIds()) {
            request->AddPortion(Request.GetGlobalContext().GetPortion(portionId));
        }
        auto dataAccessorsManager = Request.GetGlobalContext().GetDataAccessorsManager();
        request->RegisterSubscriber(std::make_shared<TColumnDataAccessorFetching>(std::move(Request), guard));
        dataAccessorsManager->AskData(request);
        return true;
    }

public:
    TDataAccessorAllocation(TBuildFilterTaskContext&& request, const ui64 mem)
        : NGroupedMemoryManager::IAllocation(mem)
        , Request(std::move(request))
    {
    }
};

}   // namespace

bool TBuildFilterTaskExecutor::ScheduleNext(TBuildFilterContext&& context) {
    if (BordersIterator.IsDone()) {
        return false;
    }

    auto bordersBatch = BordersIterator.Next();
    const ui64 mem = TColumnDataAccessorFetching::GetRequiredMemory(bordersBatch.GetPortionIds(), context);
    const ui64 processId = context.GetRequestGuard()->GetMemoryProcessId();
    const ui64 scopeId = context.GetRequestGuard()->GetMemoryScopeId();
    const ui64 groupId = context.GetRequestGuard()->GetMemoryGroupId();

    ui64 portionsCount = bordersBatch.GetPortionIds().size();

    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("component", "duplicates_manager")
        ("type", "schedule_next")
        ("portions_count", portionsCount);

    TBuildFilterTaskContext request(std::move(context), shared_from_this(), std::move(bordersBatch));
    if (portionsCount == 0) {
        AFL_VERIFY(BordersIterator.IsDone());
        TActorContext::AsActorContext().Send(context.GetOwner(),
            std::make_unique<TEvBordersConstructionResult>(std::move(request), THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>{}, nullptr));
        return true;
    }

    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId,
        { std::make_shared<TDataAccessorAllocation>(std::move(request), mem) }, (ui64)TFilterAccumulator::EFetchingStage::ACCESSORS);
    return true;
}

TMergeContext::TMergeContext(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters, const bool reversed, const std::shared_ptr<TPortionStore>& portions, const std::map<ui32, std::shared_ptr<arrow::Field>>& fetchingColumns)
    : Merger(std::move(merger))
    , Counters(std::move(counters))
    , IsReversed(reversed)
    , Portions(portions)
    , FetchingColumns(fetchingColumns) {
}

TMergeBorders::TMergeBorders(const TActorId& owner, const std::shared_ptr<TMergeContext>& context, const TEvBordersConstructionResult::TPtr& event, const std::vector<NArrow::TSimpleRow>& readyBorders)
    : Owner(owner)
    , Context(context)
    , Event(event)
    , ReadyBorders(readyBorders) {
}

void TMergeBorders::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    auto columnData = Event->Get()->Result->ExtractDataByPortion(Context->FetchingColumns);
    for (const auto& [portionId, data] : columnData) {
        Context->Merger->AddSource(data, nullptr, Context->IsReversed ? NArrow::NMerger::TIterationOrder::Reversed(0) : NArrow::NMerger::TIterationOrder::Forward(0), portionId);
        Context->FiltersBuilder.AddSource(portionId, Context->Portions->GetPortionVerified(portionId)->GetRecordsCount());
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("component", "duplicates_manager")
            ("event", "TMergeBorders::DoExecute")
            ("type", "add_source")
            ("portion_id", portionId)
            ("records_count", data->GetRecordsCount())
            ("builder", Context->FiltersBuilder.DebugString());
    }

    AFL_VERIFY(Context->FiltersBuilder.CountSources() > 0 || ReadyBorders.empty());

    for (const auto& readyBorder: ReadyBorders) {
        Context->Merger->PutControlPoint(readyBorder.BuildSortablePosition(Context->IsReversed), false);
        Context->Merger->DrainToControlPoint(Context->FiltersBuilder, true);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("component", "duplicates_manager")
            ("event", "TMergeBorders::DoExecute")
            ("type", "drain")
            ("border", readyBorder.BuildSortablePosition(Context->IsReversed).DebugString())
            ("builder", Context->FiltersBuilder.DebugString());
    }

    Context->Counters->OnRowsMerged(Context->FiltersBuilder.GetRowsAdded() - Context->PrevRowsAdded, Context->FiltersBuilder.GetRowsSkipped() - Context->PrevRowsSkipped, 0);
    Context->PrevRowsAdded = Context->FiltersBuilder.GetRowsAdded();
    Context->PrevRowsSkipped = Context->FiltersBuilder.GetRowsSkipped();

    TActivationContext::AsActorContext().Send(Owner,
        std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context), Context->FiltersBuilder.ExtractReadyFilters(), TConclusionStatus::Success()));
}

void TMergeBorders::DoOnCannotExecute(const TString& reason) {
    TActivationContext::AsActorContext().Send(Owner,
        std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context), THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason)));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
