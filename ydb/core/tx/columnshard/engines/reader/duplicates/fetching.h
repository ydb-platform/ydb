#pragma once

#include "manager.h"

#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader {

class TColumnFetchingContext {
private:
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>, Constructor);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ResultSchema);
    const NColumnShard::TCounterGuard Guard;

public:
    TColumnFetchingContext(const std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>& constructor,
        NColumnShard::TCounterGuard&& counterGuard, const TActorId& owner)
        : Owner(owner)
        , Constructor(constructor)
        , Guard(std::move(counterGuard)) {
        std::set<ui32> columnIds = Constructor->GetSource()->GetContext()->GetReadMetadata()->GetPKColumnIds();
        for (const ui32 columnId : Constructor->GetSource()->GetContext()->GetReadMetadata()->GetIndexInfo().GetSnapshotColumnIds()) {
            columnIds.emplace(columnId);
        }
        ResultSchema =
            std::make_shared<TFilteredSnapshotSchema>(Constructor->GetSource()->GetContext()->GetReadMetadata()->GetResultSchema(), columnIds);
    }

    void OnError(const TString& message) const {
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Constructor->GetSource()->GetSourceId(), TConclusionStatus::Fail(message)));
    }

    void SetResourceGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        Constructor->SetMemoryGuard(std::move(guard));
    }
    void OnResult(std::shared_ptr<NArrow::TGeneralContainer>&& result) {
        // AFL_VERIFY(result->schema ... )
        Constructor->SetColumnData(std::move(result));
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Constructor->GetSource()->GetSourceId(), TConclusionStatus::Success()));
    }
};

class TColumnsAssembleTask: public NConveyor::ITask {
private:
    std::shared_ptr<TColumnFetchingContext> Context;
    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> DataFetchers;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

    TColumnsAssembleTask(const std::shared_ptr<TColumnFetchingContext>& context,
        THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>>&& dataFetchers,
        const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard)
        : Context(context)
        , DataFetchers(std::move(dataFetchers))
        , ResourcesGuard(resourcesGuard) {
    }

private:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        NArrow::NAccessor::TAccessorsCollection accessorsResult;
        NIndexes::TIndexesCollection indexesResult;
        NCommon::TFetchingResultContext result(accessorsResult, indexesResult, Context->GetConstructor()->GetSource());
        for (auto& [columnId, fetcher] : DataFetchers) {
            fetcher->OnDataCollected(result);
        }
        auto batch = accessorsResult.ToGeneralContainer(Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetResolver());
        Context->OnResult(std::move(batch));
        return TConclusionStatus::Success();
    }
    virtual void DoOnCannotExecute(const TString& reason) override {
        Context->OnError(reason);
    }
    virtual TString GetTaskClassIdentifier() const override {
        return "TDuplicateFilterConstructor::TColumnsAssembleTask";
    }
};

class TColumnsFetcherTask: public NBlobOperations::NRead::ITask {
private:
    using TBase = NBlobOperations::NRead::ITask;

    std::shared_ptr<TColumnFetchingContext> Context;
    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> DataFetchers;

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();
        TReadActionsCollection readActions;
        for (auto&& [_, i] : DataFetchers) {
            i->OnDataReceived(readActions, blobsData);
        }
        AFL_VERIFY(blobsData.IsEmpty());
        if (readActions.IsEmpty()) {
            auto task = std::make_shared<TColumnsAssembleTask>(Context, std::move(DataFetchers), resourcesGuard);
            NConveyor::TScanServiceOperator::SendTaskToExecute(
                task, Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetConveyorProcessId());
        } else {
            std::shared_ptr<TColumnsFetcherTask> nextReadTask = std::make_shared<TColumnsFetcherTask>(std::move(readActions), Context);
            NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
        }
    }
    virtual bool DoOnError(const TString& /*storageId*/, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        Context->OnError(TStringBuilder() << "Error reading blob range for columns: " << range.ToString() << ", error: "
                                          << status.GetErrorMessage() << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus()));
        return false;
    }

public:
    TColumnsFetcherTask(const TReadActionsCollection& actions, const std::shared_ptr<TColumnFetchingContext>& context)
        : TBase(actions, "DUPLICATES", context->GetConstructor()->GetSource()->GetContext()->GetReadMetadata()->GetScanIdentifier())
        , Context(context) {
    }
};

class TColumnsMemoryAllocation: public NGroupedMemoryManager::IAllocation {
private:
    using TBase = NGroupedMemoryManager::IAllocation;

    std::shared_ptr<NBlobOperations::NRead::ITask> Action;
    YDB_READONLY_DEF(std::shared_ptr<TColumnFetchingContext>, Context);

    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
        Context->SetResourceGuard(std::move(guard));
        NActors::TActivationContext::AsActorContext().Register(new NBlobOperations::NRead::TActor(std::move(Action)));
        return true;
    }
    virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
        Context->OnError(errorMessage);
    }

public:
    TColumnsMemoryAllocation(
        const ui64 mem, const std::shared_ptr<NBlobOperations::NRead::ITask>& action, const std::shared_ptr<TColumnFetchingContext>& context)
        : TBase(mem)
        , Action(action)
        , Context(context) {
    }
};
}   // namespace NKikimr::NOlap::NReader
