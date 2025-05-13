#pragma once

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/events.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;

class TFetchingStatus {
private:
    std::atomic_uint64_t FetchingMemoryGroupId = std::numeric_limits<uint64_t>::max();
    std::atomic_bool IsFetchingStarted = false;

public:
    [[nodiscard]] bool SetStartAllocation(const ui64 memoryGroupId) {
        ui64 oldMemGroup = FetchingMemoryGroupId.load();
        while (true) {
            if (oldMemGroup < memoryGroupId) {
                return false;
            }
            if (FetchingMemoryGroupId.compare_exchange_weak(oldMemGroup, memoryGroupId)) {
                break;
            }
        }

        AFL_VERIFY(oldMemGroup != memoryGroupId);
        if (IsFetchingStarted.load()) {
            return false;
        }
        return true;
    }

    [[nodiscard]] bool SetStartFetching() {
        return !IsFetchingStarted.exchange(true);
    }
};

class TColumnFetchingContext {
private:
    enum class EState {
        FETCH_PORTION_ACCESSOR = 0,
        ALLOCATE_MEMORY,
        FETCH_BLOBS,
        ASSEMBLE_BLOBS,
    };

private:
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TFetchingStatus>, Status);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ResultSchema);
    YDB_READONLY_DEF(std::shared_ptr<NCommon::IDataSource>, Source);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TGroupGuard>, MemoryGroupGuard);

    EState State = (EState)0;
    std::optional<TPortionDataAccessor> PortionAccessor;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocatedMemory;
    THashMap<TChunkAddress, TString> Blobs;
    bool IsDone = false;

    void OnDone() {
        AFL_VERIFY(!IsDone);
        IsDone = true;
    }

    void AdvanceState(const EState expectedCurrent) {
        AFL_VERIFY(State == expectedCurrent);
        State = (EState)((ui64)State + 1);
    }

public:
    TColumnFetchingContext(const std::shared_ptr<TFetchingStatus>& status, const std::shared_ptr<NCommon::IDataSource>& source,
        const TActorId& owner, const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& memoryGroupGuard)
        : Owner(owner)
        , Status(status)
        , Source(source)
        , MemoryGroupGuard(memoryGroupGuard) {
        std::set<ui32> columnIds = source->GetContext()->GetReadMetadata()->GetPKColumnIds();
        for (const ui32 columnId : source->GetContext()->GetReadMetadata()->GetIndexInfo().GetSnapshotColumnIds()) {
            columnIds.emplace(columnId);
        }
        ResultSchema = std::make_shared<TFilteredSnapshotSchema>(source->GetContext()->GetReadMetadata()->GetResultSchema(), columnIds);
    }

    void OnError(const TString& message) {
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Source->GetSourceId(), TConclusionStatus::Fail(message), nullptr));
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
        AllocatedMemory = std::move(guard);
        AFL_VERIFY(AllocatedMemory);
    }
    void SetBlobs(THashMap<TChunkAddress, TString>&& blobData) {
        AdvanceState(EState::FETCH_BLOBS);
        Blobs = std::move(blobData);
    }
    void BuildResult() {
        AdvanceState(EState::ASSEMBLE_BLOBS);
        AFL_VERIFY(PortionAccessor);
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Source->GetSourceId(),
                       PortionAccessor->PrepareForAssemble(*ResultSchema, *ResultSchema, Blobs, Source->GetDataSnapshot())
                           .AssembleToGeneralContainer({}),
                       std::move(AllocatedMemory)));
        OnDone();
    }

    static void StartAllocation(const std::shared_ptr<TColumnFetchingContext>& context);
};

class TColumnsAssembleTask: public NConveyor::ITask {
private:
    std::shared_ptr<TColumnFetchingContext> Context;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        Context->BuildResult();
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
        THashMap<TChunkAddress, TString> blobs;
        for (const auto& [chunk, range] : Chunks) {
            AFL_VERIFY(blobs.emplace(chunk, blobsData.Extract(Context->GetSource()->GetColumnStorageId(chunk.GetColumnId()),
                                             Context->GetSource()->RestoreBlobRange(range))).second);
        }
        AFL_VERIFY(blobsData.IsEmpty());
        Context->SetBlobs(std::move(blobs));
        NConveyor::TScanServiceOperator::SendTaskToExecute(task, Context->GetSource()->GetContext()->GetCommonContext()->GetConveyorProcessId());
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
        if (!Context->GetStatus()->SetStartFetching()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicates_manager")("event", "skip_start_fetching")(
                "reason", "already_started")("manager", Context->GetOwner());
            return false;
        }
        Context->SetResourceGuard(std::move(guard));
        NActors::TActivationContext::AsActorContext().Register(new NBlobOperations::NRead::TActor(Action));
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
            { allocationTask }, (ui32)NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
    }

public:
    TPortionAccessorFetchingSubscriber(const std::shared_ptr<TColumnFetchingContext>& context)
        : Context(context) {
    }
};
}   // namespace NKikimr::NOlap::NReader::NSimple
