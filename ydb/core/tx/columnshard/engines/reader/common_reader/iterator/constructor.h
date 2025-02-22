#pragma once
#include "fetching.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader::NCommon {

class IKernelFetchLogic {
private:
    YDB_READONLY(ui32, ColumnId, 0);

    virtual void DoStart(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources, TReadActionsCollection& nextRead) = 0;
    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) = 0;
    virtual void DoOnDataCollected(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources) = 0;

protected:
    const std::shared_ptr<IDataSource> Source;

public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<IKernelFetchLogic, TString, ui32, const std::shared_ptr<IDataSource>&>;

    virtual ~IKernelFetchLogic() = default;

    IKernelFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
        : ColumnId(columnId)
        , Source(source) {
    }

    void Start(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources, TReadActionsCollection& nextRead) {
        DoStart(resources, nextRead);
    }
    void OnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        DoOnDataReceived(nextRead, blobs);
    }
    void OnDataCollected(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources) {
        DoOnDataCollected(resources);
    }
};

class TChunkRestoreInfo {
private:
    std::optional<TBlobRange> BlobRange;
    std::optional<TPortionDataAccessor::TAssembleBlobInfo> Data;
    const ui32 RecordsCount;

public:
    TChunkRestoreInfo(const ui32 recordsCount, const TBlobRange& range)
        : BlobRange(range)
        , RecordsCount(recordsCount)
    {
    }

    const std::optional<TBlobRange>& GetBlobRangeOptional() const {
        return BlobRange;
    }

    TChunkRestoreInfo(const ui32 recordsCount, const TPortionDataAccessor::TAssembleBlobInfo& defaultData)
        : Data(defaultData)
        , RecordsCount(recordsCount)
    {
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

class TDefaultFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;
    static const inline auto Registrator = TFactory::TRegistrator<TDefaultFetchLogic>("default");

    std::vector<TChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    virtual void DoOnDataCollected(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources) override {
        AFL_VERIFY(!IIndexInfo::IsSpecialColumn(GetColumnId()));
        std::vector<TPortionDataAccessor::TAssembleBlobInfo> chunks;
        for (auto&& i : ColumnChunks) {
            chunks.emplace_back(i.ExtractDataVerified());
        }

        TPortionDataAccessor::TPreparedColumn column(std::move(chunks), Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId()));
        resources->AddVerified(GetColumnId(), column.AssembleAccessor().DetachResult(), true);
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

    virtual void DoStart(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources, TReadActionsCollection& nextRead) override {
        if (resources->HasColumn(GetColumnId())) {
            return;
        }
        auto columnChunks = Source->GetStageData().GetPortionAccessor().GetColumnChunksPointers(GetColumnId());
        if (columnChunks.empty()) {
            ColumnChunks.emplace_back(
                Source->GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(Source->GetRecordsCount(),
                                                    Source->GetSourceSchema()->GetExternalDefaultValueVerified(GetColumnId())));
            return;
        }
        StorageId = Source->GetColumnStorageId(GetColumnId());
        TBlobsAction blobsAction(Source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        auto filterPtr = Source->GetStageData().GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetIterator(false, Source->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Source->RestoreBlobRange(c->BlobRange));
                ColumnChunks.emplace_back(c->GetMeta().GetRecordsCount(), Source->RestoreBlobRange(c->BlobRange));
            } else {
                ColumnChunks.emplace_back(c->GetMeta().GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(
                    c->GetMeta().GetRecordsCount(), Source->GetSourceSchema()->GetExternalDefaultValueVerified(c->GetColumnId())));
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Source->GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TDefaultFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
        : TBase(columnId, source) {
    }
};

class TColumnsFetcherTask: public NBlobOperations::NRead::ITask, public NColumnShard::TMonitoringObjectsCounter<TColumnsFetcherTask> {
private:
    using TBase = NBlobOperations::NRead::ITask;
    std::shared_ptr<IDataSource> Source;
    THashMap<ui32, std::shared_ptr<IKernelFetchLogic>> DataFetchers;
    TFetchingScriptCursor Cursor;
    NBlobOperations::NRead::TCompositeReadBlobs ProvidedBlobs;
    const NColumnShard::TCounterGuard Guard;
    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())(
            "scan_actor_id", Source->GetContext()->GetCommonContext()->GetScanActorId())("status", status.GetErrorMessage())(
            "status_code", status.GetStatus())("storage_id", storageId);
        NActors::TActorContext::AsActorContext().Send(Source->GetContext()->GetCommonContext()->GetScanActorId(),
            std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
                TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
        return false;
    }

public:
    TColumnsFetcherTask(TReadActionsCollection&& actions, const THashMap<ui32, std::shared_ptr<IKernelFetchLogic>>& fetchers,
        const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& cursor, const TString& taskCustomer,
        const TString& externalTaskId = "")
        : TBase(actions, taskCustomer, externalTaskId)
        , Source(source)
        , DataFetchers(fetchers)
        , Cursor(cursor)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetchBlobsGuard())
    {
    }
};

class TBlobsFetcherTask: public NBlobOperations::NRead::ITask, public NColumnShard::TMonitoringObjectsCounter<TBlobsFetcherTask> {
private:
    using TBase = NBlobOperations::NRead::ITask;
    const std::shared_ptr<IDataSource> Source;
    TFetchingScriptCursor Step;
    const std::shared_ptr<TSpecialReadContext> Context;
    const NColumnShard::TCounterGuard Guard;

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override;

public:
    template <class TSource>
    TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, const std::shared_ptr<TSource>& sourcePtr,
        const TFetchingScriptCursor& step, const std::shared_ptr<NCommon::TSpecialReadContext>& context, const TString& taskCustomer,
        const TString& externalTaskId)
        : TBlobsFetcherTask(readActions, std::static_pointer_cast<IDataSource>(sourcePtr), step, context, taskCustomer, externalTaskId) {
    }

    TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions,
        const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step,
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, const TString& taskCustomer, const TString& externalTaskId);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
