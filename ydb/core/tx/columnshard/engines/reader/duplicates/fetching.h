#pragma once

#include "manager.h"

#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader {

class TSimpleFetchLogic: public NCommon::IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    class TChunkRestoreInfo {
    private:
        std::optional<TBlobRange> BlobRange;
        std::optional<TPortionDataAccessor::TAssembleBlobInfo> Data;
        const ui32 RecordsCount;

    public:
        TChunkRestoreInfo(const ui32 recordsCount, const TBlobRange& range)
            : BlobRange(range)
            , RecordsCount(recordsCount) {
        }

        const std::optional<TBlobRange>& GetBlobRangeOptional() const {
            return BlobRange;
        }

        TChunkRestoreInfo(const ui32 recordsCount, const TPortionDataAccessor::TAssembleBlobInfo& defaultData)
            : Data(defaultData)
            , RecordsCount(recordsCount) {
        }

        TPortionDataAccessor::TAssembleBlobInfo ExtractDataVerified() {
            AFL_VERIFY(!!Data);
            Data->SetExpectedRecordsCount(RecordsCount);
            return std::move(*Data);
        }

        void SetBlobData(const TString& data) {
            AFL_VERIFY(!Data);
            Data.emplace(data);
        }
    };

    std::vector<TColumnRecord> ColumnChunksExt;
    std::vector<TChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;

    virtual void DoOnDataCollected(NCommon::TFetchingResultContext& context) override {
        // TODO: why? investigate in simple reader or ask
        // AFL_VERIFY(!IIndexInfo::IsSpecialColumn(GetEntityId()));
        std::vector<TPortionDataAccessor::TAssembleBlobInfo> chunks;
        for (auto&& i : ColumnChunks) {
            chunks.emplace_back(i.ExtractDataVerified());
        }

        TPortionDataAccessor::TPreparedColumn column(
            std::move(chunks), context.GetSource()->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId()));
        context.GetAccessors().AddVerified(GetEntityId(), column.AssembleAccessor().DetachResult(), true);
    }

    virtual void DoOnDataReceived(TReadActionsCollection& /*nextRead*/, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        if (ColumnChunks.empty()) {
            return;
        }
        for (auto&& i : ColumnChunks) {
            if (!i.GetBlobRangeOptional()) {
                continue;
            }
            AFL_VERIFY(!!StorageId);
            i.SetBlobData(blobs.Extract(*StorageId, *i.GetBlobRangeOptional()));
        }
    }

    virtual void DoStart(TReadActionsCollection& nextRead, NCommon::TFetchingResultContext& context) override {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "duplicate_filter_manager")("event", "start_fetching_columns")(
            "column_id", GetEntityId())("column_chunks", ColumnChunksExt.size());
        auto source = context.GetSource();
        if (ColumnChunksExt.empty()) {
            ColumnChunks.emplace_back(source->GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(source->GetRecordsCount(),
                                                                     source->GetSourceSchema()->GetExternalDefaultValueVerified(GetEntityId())));
            return;
        }
        StorageId = source->GetColumnStorageId(GetEntityId());
        TBlobsAction blobsAction(source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        for (auto&& c : ColumnChunksExt) {
            reading->SetIsBackgroundProcess(false);
            reading->AddRange(source->RestoreBlobRange(c.BlobRange));
            ColumnChunks.emplace_back(c.GetMeta().GetRecordsCount(), source->RestoreBlobRange(c.BlobRange));
        }
        ColumnChunksExt.clear();
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TSimpleFetchLogic(const ui32 entityId, const std::shared_ptr<IStoragesManager>& storagesManager, std::vector<TColumnRecord>&& columnChunks)
        : TBase(entityId, storagesManager)
        , ColumnChunksExt(std::move(columnChunks)) {
    }
};

class TColumnFetchingContext {
private:
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<TDuplicateFilterConstructor::TSourceFilterConstructor>, Constructor);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ResultSchema);
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

    void SetResourceGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        Constructor->SetMemoryGuard(std::move(guard));
    }
    void OnResult(std::shared_ptr<NArrow::TGeneralContainer>&& result) {
        // AFL_VERIFY(result->schema ... )
        Constructor->SetColumnData(std::move(result));
        TActorContext::AsActorContext().Send(
            Owner, new TEvDuplicateFilterDataFetched(Constructor->GetSource()->GetSourceId(), TConclusionStatus::Success()));
        OnDone();
    }
};

class TColumnsAssembleTask: public NConveyor::ITask {
private:
    std::shared_ptr<TColumnFetchingContext> Context;
    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> DataFetchers;
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;

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

public:
    TColumnsAssembleTask(const std::shared_ptr<TColumnFetchingContext>& context,
        THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>>&& dataFetchers,
        const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard)
        : Context(context)
        , DataFetchers(std::move(dataFetchers))
        , ResourcesGuard(resourcesGuard) {
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
            std::shared_ptr<TColumnsFetcherTask> nextReadTask =
                std::make_shared<TColumnsFetcherTask>(std::move(readActions), Context, std::move(DataFetchers));
            NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
        }
    }
    virtual bool DoOnError(const TString& /*storageId*/, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        Context->OnError(TStringBuilder() << "Error reading blob range for columns: " << range.ToString() << ", error: "
                                          << status.GetErrorMessage() << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus()));
        return false;
    }

public:
    TColumnsFetcherTask(const TReadActionsCollection& actions, const std::shared_ptr<TColumnFetchingContext>& context,
        THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>>&& fetchers)
        : TBase(actions, "DUPLICATES", context->GetConstructor()->GetSource()->GetContext()->GetReadMetadata()->GetScanIdentifier())
        , Context(context)
        , DataFetchers(std::move(fetchers)) {
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
        const TPortionDataAccessor& portionAccessor = result.GetPortionAccessorVerified(Context->GetConstructor()->GetSource()->GetSourceId());

        const std::set<ui32> columnIds(Context->GetResultSchema()->GetColumnIds().begin(), Context->GetResultSchema()->GetColumnIds().end());
        THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> fetchers;
        TReadActionsCollection readActions;

        for (const ui32 columnId : columnIds) {
            std::vector<TColumnRecord> columnCunks;
            for (const auto* record : portionAccessor.GetColumnChunksPointers(columnId)) {
                columnCunks.emplace_back(*record);
            }

            std::shared_ptr<NCommon::IKernelFetchLogic> fetcher = std::make_shared<TSimpleFetchLogic>(columnId,
                Context->GetConstructor()->GetSource()->GetContext()->GetCommonContext()->GetStoragesManager(), std::move(columnCunks));

            {
                NArrow::NAccessor::TAccessorsCollection emptyTable;
                NIndexes::TIndexesCollection emptyIndexes;
                NCommon::TFetchingResultContext contextFetch(emptyTable, emptyIndexes, Context->GetConstructor()->GetSource());
                fetcher->Start(readActions, contextFetch);
            }
            fetchers.emplace(columnId, fetcher);
        }
        AFL_VERIFY(!readActions.IsEmpty());
        auto fetchingTask = std::make_shared<TColumnsFetcherTask>(std::move(readActions), Context, std::move(fetchers));

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
