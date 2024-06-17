#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/blob.h>
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

class TBlobsFetcherTask: public NBlobOperations::NRead::ITask, public NColumnShard::TMonitoringObjectsCounter<TBlobsFetcherTask> {
private:
    using TBase = NBlobOperations::NRead::ITask;
    const std::shared_ptr<IDataSource> Source;
    TFetchingScriptCursor Step;
    const std::shared_ptr<TSpecialReadContext> Context;

    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override;
public:
    TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, const std::shared_ptr<IDataSource>& sourcePtr,
        const TFetchingScriptCursor& step, const std::shared_ptr<TSpecialReadContext>& context, const TString& taskCustomer, const TString& externalTaskId)
        : TBase(readActions, taskCustomer, externalTaskId) 
        , Source(sourcePtr)
        , Step(step)
        , Context(context)
    {

    }
};

}
