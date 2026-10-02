#pragma once
#include "fetching.h"
#include "source.h"

#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/collection.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingResultContext {
private:
    NArrow::NAccessor::TAccessorsCollection& Accessors;
    NIndexes::TIndexesCollection& Indexes;
    IDataSource& Source;
    std::optional<std::shared_ptr<NArrow::TColumnFilter>> AppliedFilter;

public:
    NArrow::NAccessor::TAccessorsCollection& GetAccessors() {
        return Accessors;
    }

    NIndexes::TIndexesCollection& GetIndexes() const {
        return Indexes;
    }

    IDataSource& GetSource() const {
        return Source;
    }

    ui32 GetRecordsCount() const {
        return Source.GetPortionAccessor().GetPortionInfo().GetRecordsCount();
    }

    const std::shared_ptr<NArrow::TColumnFilter>& GetAppliedFilter() const {
        if (AppliedFilter) {
            return *AppliedFilter;
        } else {
            return Accessors.GetAppliedFilter();
        }
    }

    TFetchingResultContext(NArrow::NAccessor::TAccessorsCollection& accessors, NIndexes::TIndexesCollection& indexes, IDataSource& source,
        const std::optional<std::shared_ptr<NArrow::TColumnFilter>>& appliedFilter = std::nullopt)
        : Accessors(accessors)
        , Indexes(indexes)
        , Source(source)
        , AppliedFilter(appliedFilter)
    {
    }
};

class IKernelFetchLogic: public NArrow::NSSA::IFetchLogic {
private:
    using TBase = NArrow::NSSA::IFetchLogic;
    virtual void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) = 0;
    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) = 0;
    virtual TConclusionStatus DoOnDataCollected(TFetchingResultContext& context) = 0;

protected:
    const std::shared_ptr<IStoragesManager> StoragesManager;

public:
    virtual ~IKernelFetchLogic() = default;

    IKernelFetchLogic(const ui32 entityId, const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(entityId)
        , StoragesManager(storagesManager)
    {
        AFL_VERIFY(StoragesManager);
    }

    void Start(TReadActionsCollection& nextRead, TFetchingResultContext& context) {
        DoStart(nextRead, context);
    }

    void OnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
        DoOnDataReceived(nextRead, blobs);
    }

    TConclusionStatus OnDataCollected(TFetchingResultContext& context) {
        return DoOnDataCollected(context);
    }
};

class TColumnsFetcherTask: public NBlobOperations::NRead::ITask, public NColumnShard::TMonitoringObjectsCounter<TColumnsFetcherTask> {
private:
    using TBase = NBlobOperations::NRead::ITask;
    std::unique_ptr<TDataSourceLease> SourceLease;
    THashMap<ui32, std::shared_ptr<IKernelFetchLogic>> DataFetchers;
    TFetchingScriptCursor Cursor;
    NBlobOperations::NRead::TCompositeReadBlobs ProvidedBlobs;
    NColumnShard::TCounterGuard Guard;
    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;

    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override;

public:
    class TStartJob: public IAsyncJob {
    private:
        TReadActionsCollection ReadActions;
        const THashMap<ui32, std::shared_ptr<IKernelFetchLogic>> Fetchers;
        const TFetchingScriptCursor Cursor;
        const TString TaskCustomer;

    public:
        TStartJob(TReadActionsCollection&& readActions, const THashMap<ui32, std::shared_ptr<IKernelFetchLogic>>& fetchers,
            const TFetchingScriptCursor& cursor, const TString& taskCustomer)
            : ReadActions(std::move(readActions))
            , Fetchers(fetchers)
            , Cursor(cursor)
            , TaskCustomer(taskCustomer)
        {
        }

        virtual void Start(std::unique_ptr<TDataSourceLease> sourceLease) override;
    };

    TColumnsFetcherTask(TReadActionsCollection&& actions, const THashMap<ui32, std::shared_ptr<IKernelFetchLogic>>& fetchers,
        std::unique_ptr<TDataSourceLease> sourceLease, const TFetchingScriptCursor& cursor, const TString& taskCustomer,
        const TString& externalTaskId = "")
        : TBase(actions, taskCustomer, externalTaskId)
        , SourceLease(std::move(sourceLease))
        , DataFetchers(fetchers)
        , Cursor(cursor)
        , Guard(SourceLease->GetSource().GetContext()->GetCommonContext()->GetCounters().GetFetchBlobsGuard())
    {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, SourceLease->GetSource().AddEvent("scf"));
    }
};

class TBlobsFetcherTask: public NBlobOperations::NRead::ITask, public NColumnShard::TMonitoringObjectsCounter<TBlobsFetcherTask> {
private:
    using TBase = NBlobOperations::NRead::ITask;
    std::unique_ptr<TDataSourceLease> SourceLease;
    TFetchingScriptCursor Step;
    const std::shared_ptr<TSpecialReadContext> Context;
    NColumnShard::TCounterGuard Guard;

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override;

public:
    class TStartJob: public IAsyncJob {
    private:
        const std::vector<std::shared_ptr<IBlobsReadingAction>> ReadActions;
        const TFetchingScriptCursor Step;
        const TString TaskCustomer;

    public:
        TStartJob(
            const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, const TFetchingScriptCursor& step, const TString& taskCustomer)
            : ReadActions(readActions)
            , Step(step)
            , TaskCustomer(taskCustomer)
        {
        }

        virtual void Start(std::unique_ptr<TDataSourceLease> sourceLease) override;
    };

    TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, std::unique_ptr<TDataSourceLease> sourceLease,
        const TFetchingScriptCursor& step, const TString& taskCustomer);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
