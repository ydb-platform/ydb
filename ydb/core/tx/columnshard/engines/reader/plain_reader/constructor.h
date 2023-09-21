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
    NActors::TActorId ScanActorId;
    const ui32 SourceIdx;
    std::shared_ptr<const TReadMetadata> ReadMetadata;
    TReadContext Context;
    THashMap<TBlobRange, ui32> NullBlocks;
    virtual bool DoOnError(const TBlobRange& range) override;
public:
    IFetchTaskConstructor(IDataReader& reader, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks, const IDataSource& source)
        : TBase(readActions)
        , ScanActorId(NActors::TActorContext::AsActorContext().SelfID)
        , SourceIdx(source.GetSourceIdx())
        , ReadMetadata(reader.GetReadMetadata())
        , Context(reader.GetContext())
        , NullBlocks(std::move(nullBlocks))
    {

    }
};

class TCommittedColumnsTaskConstructor: public IFetchTaskConstructor {
private:
    TCommittedBlob CommittedBlob;
    using TBase = IFetchTaskConstructor;
protected:
    virtual void DoOnDataReady() override;
public:
    TCommittedColumnsTaskConstructor(IDataReader& reader, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const TCommittedDataSource& source)
        : TBase(reader, readActions, std::move(nullBlocks), source)
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
    TPortionInfo::TPreparedBatchData BuildBatchAssembler();
public:
    TAssembleColumnsTaskConstructor(IDataReader& reader, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion)
        : TBase(reader, readActions, std::move(nullBlocks), portion)
        , ColumnIds(columnIds)
        , PortionInfo(portion.GetPortionInfoPtr())
    {

    }
};

class TFFColumnsTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    using TBase = TAssembleColumnsTaskConstructor;
    std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
    virtual void DoOnDataReady() override;
public:
    TFFColumnsTaskConstructor(IDataReader& reader, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion)
        : TBase(reader, readActions, std::move(nullBlocks), columnIds, portion)
        , AppliedFilter(portion.GetFilterStageData().GetAppliedFilter())
    {
    }
};

class TEFTaskConstructor: public TAssembleColumnsTaskConstructor {
private:
    bool UseEarlyFilter = false;
    using TBase = TAssembleColumnsTaskConstructor;
    virtual void DoOnDataReady() override;
public:
    TEFTaskConstructor(IDataReader& reader, const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions, THashMap<TBlobRange, ui32>&& nullBlocks,
        const std::set<ui32>& columnIds, const TPortionDataSource& portion, const bool useEarlyFilter)
        : TBase(reader, readActions, std::move(nullBlocks), columnIds, portion)
        , UseEarlyFilter(useEarlyFilter)
    {
    }
};

}
