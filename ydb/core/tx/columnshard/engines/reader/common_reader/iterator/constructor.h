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

    virtual void DoStart(TReadActionsCollection& nextRead) = 0;
    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) = 0;
    virtual void DoOnDataCollected() = 0;

protected:
    const std::shared_ptr<IDataSource> Source;
    const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection> Resources;

public:
    virtual ~IKernelFetchLogic() = default;

    IKernelFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
        : ColumnId(columnId)
        , Source(source) {
    }

    void Start(TReadActionsCollection& nextRead) {
        DoStart(nextRead);
    }
    void OnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        DoOnDataReceived(nextRead, blobs);
    }
    void OnDataCollected() {
        DoOnDataCollected();
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
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetchBlobsGuard()) {
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
