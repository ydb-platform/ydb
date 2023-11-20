#pragma once
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/blob.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

class IFetchTaskConstructor: public NBlobOperations::NRead::ITask {
private:
    using TBase = NBlobOperations::NRead::ITask;
protected:
    const std::shared_ptr<IDataSource> Source;
    const std::shared_ptr<TSpecialReadContext> Context;
    THashMap<TBlobRange, ui32> NullBlocks;
    NColumnShard::TCounterGuard TasksGuard;
    virtual bool DoOnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override;
public:
    IFetchTaskConstructor(const std::shared_ptr<TSpecialReadContext>& context, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::shared_ptr<IDataSource>& sourcePtr, const TString& taskCustomer)
        : TBase(readActions, taskCustomer)
        , Source(sourcePtr)
        , Context(context)
        , NullBlocks(std::move(nullBlocks))
        , TasksGuard(context->GetCommonContext()->GetCounters().GetReadTasksGuard())
    {
    }
};

class TCommittedColumnsTaskConstructor: public IFetchTaskConstructor {
private:
    TCommittedBlob CommittedBlob;
    using TBase = IFetchTaskConstructor;
protected:
    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
public:
    TCommittedColumnsTaskConstructor(const std::shared_ptr<TSpecialReadContext>& context, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const TCommittedDataSource& source, const std::shared_ptr<IDataSource>& sourcePtr, const TString& taskCustomer)
        : TBase(context, readActions, std::move(nullBlocks), sourcePtr, taskCustomer)
        , CommittedBlob(source.GetCommitted())
    {

    }
};

class TAssembleColumnsTaskConstructor: public IFetchTaskConstructor {
private:
    using TBase = IFetchTaskConstructor;
protected:
    std::set<ui32> ColumnIds;
    std::shared_ptr<TPortionInfo> PortionInfo;
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> BuildBatchAssembler();
public:
    TAssembleColumnsTaskConstructor(const std::shared_ptr<TSpecialReadContext>& context, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion, const std::shared_ptr<IDataSource>& sourcePtr, const TString& taskCustomer)
        : TBase(context, readActions, std::move(nullBlocks), sourcePtr, taskCustomer)
        , ColumnIds(columnIds)
        , PortionInfo(portion.GetPortionInfoPtr())
    {

    }
};

class TFFColumnsTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    using TBase = TAssembleColumnsTaskConstructor;
    std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
public:
    TFFColumnsTaskConstructor(const std::shared_ptr<TSpecialReadContext>& context, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion, const std::shared_ptr<IDataSource>& sourcePtr, const TString& taskCustomer)
        : TBase(context, readActions, std::move(nullBlocks), columnIds, portion, sourcePtr, taskCustomer)
        , AppliedFilter(portion.GetFilterStageData().GetAppliedFilter())
    {
    }
};

class TEFTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    bool UseEarlyFilter = false;
    using TBase = TAssembleColumnsTaskConstructor;
    virtual void DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override;
public:
    TEFTaskConstructor(const std::shared_ptr<TSpecialReadContext>& context, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion, const std::shared_ptr<IDataSource>& sourcePtr, const bool useEarlyFilter, const TString& taskCustomer)
        : TBase(context, readActions, std::move(nullBlocks), columnIds, portion, sourcePtr, taskCustomer)
        , UseEarlyFilter(useEarlyFilter)
    {
    }
};

}
