#include "executor.h"
#include "merge.h"
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
                new NPrivate::TEvFilterConstructionResult(
                    TConclusionStatus::Fail(errorAddresses.GetErrorMessage()), Request.GetGlobalContext().MakeResultInFlightGuard()));
            return;
        }

        AFL_VERIFY(AllocationGuard);
        const std::shared_ptr<TBuildDuplicateFilters> task =
            std::make_shared<TBuildDuplicateFilters>(std::move(Request), std::move(objectAddresses), std::move(AllocationGuard));
        NConveyorComposite::TDeduplicationServiceOperator::SendTaskToExecute(task);
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
            Request.GetGlobalContext().GetOwner(), new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(errorMessage),
                                                       Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
};

class TColumnDataAllocation: public NGroupedMemoryManager::IAllocation {
private:
    TBuildFilterTaskContext Request;

private:
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        TActorContext::AsActorContext().Send(Request.GetGlobalContext().GetOwner(),
            new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage),
                Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        THashSet<TPortionAddress> portionAddresses;
        for (const ui64 portionId : Request.GetRequiredPortions()) {
            AFL_VERIFY(portionAddresses.emplace(Request.GetGlobalContext().GetPortion(portionId)->GetAddress()).second);
        }
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
                new NPrivate::TEvFilterConstructionResult(
                    TConclusionStatus::Fail(result.GetErrorMessage()), Request.GetGlobalContext().MakeResultInFlightGuard()));
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
            new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(TStringBuilder() << "cannot allocate memory: " << errorMessage),
                Request.GetGlobalContext().MakeResultInFlightGuard()));
    }
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NBlobOperations::EConsumer::DUPLICATE_FILTERING);
        for (const ui64 portionId : Request.GetRequiredPortions()) {
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
    std::vector<TIntervalInfo> intervals;
    THashSet<ui64> portionIds;

    while (portionIds.size() < BATCH_PORTIONS_COUNT_SOFT_LIMIT && Portions.Next()) {
        intervals.emplace_back(Portions.GetCurrentInterval());
        for (const ui64 portion : Portions.GetCurrentPortionIds()) {
            portionIds.emplace(portion);
        }
    }

    AFL_VERIFY(intervals.empty() == portionIds.empty());
    if (intervals.empty()) {
        AFL_VERIFY(Portions.IsDone());
        return false;
    }

    const ui64 mem = TColumnDataAccessorFetching::GetRequiredMemory(portionIds, context);
    const ui64 processId = context.GetRequestGuard()->GetMemoryProcessId();
    const ui64 scopeId = context.GetRequestGuard()->GetMemoryScopeId();
    const ui64 groupId = context.GetRequestGuard()->GetMemoryGroupId();
    NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::SendToAllocation(processId, scopeId, groupId,
        { std::make_shared<TDataAccessorAllocation>(TBuildFilterTaskContext(std::move(context), shared_from_this(), std::move(intervals),
                                                        std::move(portionIds)), mem) }, (ui64)TFilterAccumulator::EFetchingStage::ACCESSORS);
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
