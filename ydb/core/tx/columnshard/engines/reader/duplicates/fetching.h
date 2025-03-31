#pragma once

#include "manager.h"

#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader {

class TColumnFetchingContext {
private:
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>, Constructor);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ResultSchema);
    std::optional<TPortionDataAccessor> PortionAccessor;
    THashMap<TChunkAddress, TString> Blobs;
    bool IsDone = false;

    void OnDone() {
        AFL_VERIFY(!IsDone);
        IsDone = true;
    }

public:
    TColumnFetchingContext(const std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>& constructor, const TActorId& owner)
        : Owner(owner)
        , Constructor(constructor) {
        std::set<ui32> columnIds = Constructor->GetSource()->GetContext()->GetReadMetadata()->GetPKColumnIds();
        for (const ui32 columnId : Constructor->GetSource()->GetContext()->GetReadMetadata()->GetIndexInfo().GetSnapshotColumnIds()) {
            columnIds.emplace(columnId);
        }
        ResultSchema =
            std::make_shared<TFilteredSnapshotSchema>(Constructor->GetSource()->GetContext()->GetReadMetadata()->GetResultSchema(), columnIds);
    }

    void OnError(const TString& message) {
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Constructor->GetSource()->GetSourceId(), TConclusionStatus::Fail(message)));
        OnDone();
    }

    void SetPortionAccessor(TPortionDataAccessor&& portionAccessor) {
        AFL_VERIFY(!PortionAccessor);
        PortionAccessor = std::move(portionAccessor);
    }
    void SetResourceGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        Constructor->SetMemoryGuard(std::move(guard));
    }
    void BuildResult() {
        // AFL_VERIFY(result->schema ... )
        AFL_VERIFY(PortionAccessor);
        Constructor->SetColumnData(
            PortionAccessor->PrepareForAssemble(*ResultSchema, *ResultSchema, Blobs, Constructor->GetSource()->GetDataSnapshot())
                .AssembleToGeneralContainer({})
                .DetachResult());
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Constructor->GetSource()->GetSourceId(), TConclusionStatus::Success()));
        OnDone();
    }
    void AddBlobs(THashMap<TChunkAddress, TString>&& blobData) {
        for (auto&& i : blobData) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }
};

class TColumnsAssembleTask: public NConveyor::ITask {
private:
    std::shared_ptr<TColumnFetchingContext> Context;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

private:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        Context->BuildResult();
        return TConclusionStatus::Success();
    }
    virtual void DoOnCannotExecute(const TString& reason) override {
        Context->OnError(reason);
    }
    virtual TString GetTaskClassIdentifier() const override {
        return "TDuplicateFilterConstructor::TColumnsAssembleTask";
    }

public:
    TColumnsAssembleTask(const std::shared_ptr<TColumnFetchingContext>& context,
        const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard)
        : Context(context)
        , ResourcesGuard(resourcesGuard) {
    }
};

class TColumnsFetcherTask: public NBlobOperations::NRead::ITask {
private:
    using TBase = NBlobOperations::NRead::ITask;

    std::shared_ptr<TColumnFetchingContext> Context;
    THashMap<TChunkAddress, TBlobRangeLink16> Chunks;

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();

        auto task = std::make_shared<TColumnsAssembleTask>(Context, resourcesGuard);
        for (const auto& [chunk, range] : Chunks) {
            Context->AddBlobs({ { chunk, blobsData.Extract(Context->GetConstructor()->GetSource()->GetColumnStorageId(chunk.GetColumnId()),
                                             Context->GetConstructor()->GetSource()->RestoreBlobRange(range)) } });
        }
        AFL_VERIFY(blobsData.IsEmpty());
        NConveyor::TScanServiceOperator::SendTaskToExecute(
            task, Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetConveyorProcessId());
    }
    virtual bool DoOnError(const TString& /*storageId*/, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        Context->OnError(TStringBuilder() << "Error reading blob range for columns: " << range.ToString() << ", error: "
                                          << status.GetErrorMessage() << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus()));
        return false;
    }

public:
    TColumnsFetcherTask(const TReadActionsCollection& actions, const std::shared_ptr<TColumnFetchingContext>& context,
        THashMap<TChunkAddress, TBlobRangeLink16>&& chunks)
        : TBase(actions, "DUPLICATES", context->GetConstructor()->GetSource()->GetContext()->GetReadMetadata()->GetScanIdentifier())
        , Context(context)
        , Chunks(std::move(chunks)) {
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

class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<TColumnFetchingContext> Context;
    ui64 MemoryGroupId;

    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetAbortionFlag();
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(!result.HasErrors());
        AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
        TPortionDataAccessor portionAccessor = std::move(result.ExtractPortionsVector()[0]);

        const std::set<ui32> columnIds(Context->GetResultSchema()->GetColumnIds().begin(), Context->GetResultSchema()->GetColumnIds().end());
        THashMap<TChunkAddress, TBlobRangeLink16> chunkRanges;
        TReadActionsCollection readActions;

        for (const ui32 columnId : columnIds) {
            std::vector<TColumnRecord> columnChunks;
            for (const auto* record : portionAccessor.GetColumnChunksPointers(columnId)) {
                columnChunks.emplace_back(*record);
                chunkRanges.emplace(record->GetAddress(), record->GetBlobRange());
            }
            if (columnChunks.empty()) {
                continue;
            }

            auto source = Context->GetConstructor()->GetSource();
            TBlobsAction blobsAction(source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
            auto reading = blobsAction.GetReading(source->GetColumnStorageId(columnId));
            for (auto&& c : columnChunks) {
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(source->RestoreBlobRange(c.BlobRange));
            }
            for (auto&& i : blobsAction.GetReadingActions()) {
                readActions.Add(i);
            }
        }
        AFL_VERIFY(!readActions.IsEmpty());
        Context->SetPortionAccessor(std::move(portionAccessor));
        auto fetchingTask = std::make_shared<TColumnsFetcherTask>(std::move(readActions), Context, std::move(chunkRanges));

        const ui64 mem = portionAccessor.GetColumnRawBytes(columnIds, false);
        auto allocationTask = std::make_shared<TColumnsMemoryAllocation>(mem, fetchingTask, Context);
        NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(
            Context->GetConstructor()->GetSource()->GetContext()->GetProcessMemoryControlId(),
            Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetScanId(), MemoryGroupId, { allocationTask },
            (ui32)NCommon::EStageFeaturesIndexes::Filter);
    }

public:
    TPortionAccessorFetchingSubscriber(const std::shared_ptr<TColumnFetchingContext>& context, const ui64 memoryGroupId)
        : Context(context)
        , MemoryGroupId(memoryGroupId) {
    }
};
}   // namespace NKikimr::NOlap::NReader
