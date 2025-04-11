#pragma once

#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader {

class TColumnFetchingContext {
private:
    enum class EState {
        FETCH_PORTION_ACCESSOR = 0,
        ALLOCATE_MEMORY,
        INIT_CONSTRUCTOR,
        FETCH_BLOBS,
        ASSEMBLE_BLOBS,
    };

private:
    EState State = (EState)0;
    YDB_READONLY_DEF(ui64, SourceId);
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TDuplicateFilterConstructor::TWaitingSourceInfo>, WaitingInfo);
    std::optional<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> AllocatedMemory;
    std::optional<std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>> Constructor;
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ResultSchema);
    std::optional<TPortionDataAccessor> PortionAccessor;
    THashMap<TChunkAddress, TString> Blobs;
    bool IsDone = false;
    YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TGroupGuard>, MemoryGroupGuard);

    void OnDone() {
        AFL_VERIFY(!IsDone);
        IsDone = true;
    }

    void AdvanceState(const EState expectedCurrent) {
        AFL_VERIFY(State == expectedCurrent);
        State = (EState)((ui64)State + 1);
    }

public:
    TColumnFetchingContext(const std::shared_ptr<TDuplicateFilterConstructor::TWaitingSourceInfo>& info,
        const TActorId& owner, const std::shared_ptr<NSimple::TSpecialReadContext>& context, const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& memoryGroupGuard)
        : Owner(owner)
        , WaitingInfo(info)
        , Source(info->Construct(context)),
        MemoryGroupGuard(memoryGroupGuard) {
        std::set<ui32> columnIds = context->GetReadMetadata()->GetPKColumnIds();
        for (const ui32 columnId : context->GetReadMetadata()->GetIndexInfo().GetSnapshotColumnIds()) {
            columnIds.emplace(columnId);
        }
        ResultSchema = std::make_shared<TFilteredSnapshotSchema>(context->GetReadMetadata()->GetResultSchema(), columnIds);
    }

    void OnError(const TString& message) {
        TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterDataFetched(Source->GetSourceId(), TConclusionStatus::Fail(message)));
        OnDone();
    }

    void SetPortionAccessor(TPortionDataAccessor&& portionAccessor) {
        AdvanceState(EState::FETCH_PORTION_ACCESSOR);
        AFL_VERIFY(!PortionAccessor);
        PortionAccessor = std::move(portionAccessor);
    }
    void SetResourceGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        AdvanceState(EState::ALLOCATE_MEMORY);
        AFL_VERIFY(!AllocatedMemory);
        AFL_VERIFY(!Constructor);
        AllocatedMemory = std::move(guard);
    }
    void SetConstructor(const std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>& constructor) {
        AdvanceState(EState::INIT_CONSTRUCTOR);
        AFL_VERIFY(!Constructor);
        Constructor = constructor;
        AFL_VERIFY(*Constructor);

        AFL_VERIFY(AllocatedMemory);
        (*Constructor)->SetMemoryGuard(std::move(*AllocatedMemory));
        AllocatedMemory->reset();
    }
    void ContinueFetching(const std::shared_ptr<NBlobOperations::NRead::ITask>& action) {
        AFL_VERIFY(State == EState::FETCH_BLOBS);
        NActors::TActivationContext::AsActorContext().Register(new NBlobOperations::NRead::TActor(std::move(action)));
    }
    void AddBlobs(THashMap<TChunkAddress, TString>&& blobData) {
        AdvanceState(EState::FETCH_BLOBS);
        for (auto&& i : blobData) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }
    void BuildResult() {
        AdvanceState(EState::ASSEMBLE_BLOBS);
        AFL_VERIFY(PortionAccessor);
        AFL_VERIFY(Constructor);
        (*Constructor)
            ->SetColumnData(PortionAccessor->PrepareForAssemble(*ResultSchema, *ResultSchema, Blobs, Source->GetDataSnapshot())
                                .AssembleToGeneralContainer({})
                                .DetachResult());
        TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterDataFetched(Source->GetSourceId(), TConclusionStatus::Success()));
        OnDone();
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
            Context->AddBlobs({ { chunk, blobsData.Extract(Context->GetSource()->GetColumnStorageId(chunk.GetColumnId()),
                                             Context->GetSource()->RestoreBlobRange(range)) } });
        }
        AFL_VERIFY(blobsData.IsEmpty());
        NConveyor::TScanServiceOperator::SendTaskToExecute(
            task, Context->GetSource()->GetContext()->GetCommonContext()->GetConveyorProcessId());
    }
    virtual bool DoOnError(const TString& /*storageId*/, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        Context->OnError(TStringBuilder() << "Error reading blob range for columns: " << range.ToString() << ", error: "
                                          << status.GetErrorMessage() << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus()));
        return false;
    }

public:
    TColumnsFetcherTask(const TReadActionsCollection& actions, const std::shared_ptr<TColumnFetchingContext>& context,
        THashMap<TChunkAddress, TBlobRangeLink16>&& chunks)
        : TBase(actions, "DUPLICATES", context->GetSource()->GetContext()->GetReadMetadata()->GetScanIdentifier())
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
        if (!Context->GetWaitingInfo()->SetStartFetching()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "skip_start_fetching")(
                "reason", "already_started")("manager", Context->GetOwner());
            return false;
        }
        Context->SetResourceGuard(std::move(guard));
        TActorContext::AsActorContext().Send(Context->GetOwner(), new TEvDuplicateFilterStartFetching(Context, Action));
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

    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Context->GetSource()->GetContext()->GetCommonContext()->GetAbortionFlag();
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

            auto source = Context->GetSource();
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
        NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(Context->GetSource()->GetContext()->GetProcessMemoryControlId(),
            Context->GetSource()->GetContext()->GetCommonContext()->GetScanId(), Context->GetMemoryGroupGuard()->GetGroupId(),
            { allocationTask }, (ui32)NCommon::EStageFeaturesIndexes::Filter);
    }

public:
    TPortionAccessorFetchingSubscriber(const std::shared_ptr<TColumnFetchingContext>& context)
        : Context(context) {
    }
};
}   // namespace NKikimr::NOlap::NReader
