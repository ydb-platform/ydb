#pragma once
#include <ydb/core/tx/columnshard/engines/reader/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include "source.h"
#include "columns_set.h"

namespace NKikimr::NOlap::NPlainReader {

class TAssemblerCommon: public NColumnShard::IDataTasksProcessor::ITask {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
protected:
    const std::shared_ptr<TSpecialReadContext> Context;
    const std::shared_ptr<IDataSource> Source;
    const std::shared_ptr<TPortionInfo> PortionInfo;
    const TReadMetadata::TConstPtr ReadMetadata;
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> Blobs;

    TPortionInfo::TPreparedBatchData BuildBatchConstructor(const ISnapshotSchema& resultSchema) {
        auto blobSchema = ReadMetadata->GetLoadSchema(PortionInfo->GetMinSnapshot());
        return PortionInfo->PrepareForAssemble(*blobSchema, resultSchema, Blobs);
    }

public:
    TAssemblerCommon(const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<TPortionInfo>& portionInfo,
        const std::shared_ptr<IDataSource>& source, const THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo>& blobs)
        : TBase(context->GetCommonContext()->GetScanActorId())
        , Context(context)
        , Source(source)
        , PortionInfo(portionInfo)
        , ReadMetadata(Context->GetReadMetadata())
        , Blobs(blobs)
    {
        AFL_VERIFY(Blobs.size());
    }

};

class TAssembleFilter: public TAssemblerCommon, public NColumnShard::TMonitoringObjectsCounter<TAssembleFilter, true, true> {
private:
    using TBase = TAssemblerCommon;

    std::shared_ptr<arrow::RecordBatch> FilteredBatch;
    std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
    std::shared_ptr<NArrow::TColumnFilter> EarlyFilter;
    const TSnapshot RecordsMaxSnapshot;
    ui32 OriginalCount = 0;
    std::shared_ptr<TColumnsSet> FilterColumns;
    const bool UseFilter = true;
    const NColumnShard::TCounterGuard TaskGuard;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
    virtual bool DoExecute() override;
public:

    virtual TString GetTaskClassIdentifier() const override {
        return "PlainReading::TAssembleFilter";
    }

    TAssembleFilter(const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<TPortionInfo>& portionInfo,
        const std::shared_ptr<IDataSource>& source, const std::shared_ptr<TColumnsSet>& filterColumns, const bool useFilter, const THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo>& blobs)
        : TBase(context, portionInfo, source, std::move(blobs))
        , RecordsMaxSnapshot(PortionInfo->RecordSnapshotMax())
        , FilterColumns(filterColumns)
        , UseFilter(useFilter)
        , TaskGuard(Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard())
    {
        Y_UNUSED(RecordsMaxSnapshot);
        TBase::SetPriority(TBase::EPriority::Normal);
    }
};

}
