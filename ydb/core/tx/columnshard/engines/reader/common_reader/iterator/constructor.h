#pragma once
#include "fetching.h"
#include "source.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader::NCommon {

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
