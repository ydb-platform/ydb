#pragma once
#include <ydb/core/tx/columnshard/engines/reader/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

    class TAssembleFilter: public NColumnShard::IDataTasksProcessor::ITask, public NColumnShard::TMonitoringObjectsCounter<TAssembleFilter, true, true> {
    private:
        using TBase = NColumnShard::IDataTasksProcessor::ITask;
        TPortionInfo::TPreparedBatchData BatchConstructor;
        std::shared_ptr<arrow::RecordBatch> FilteredBatch;
        const ui32 SourceIdx;
        TReadMetadata::TConstPtr ReadMetadata;
        std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
        std::shared_ptr<NArrow::TColumnFilter> EarlyFilter;
        ui32 OriginalCount = 0;
        bool AllowEarlyFilter = false;
        std::set<ui32> FilterColumnIds;
        const bool UseFilter = true;
    protected:
        virtual bool DoApply(IDataReader& owner) const override;
        virtual bool DoExecute() override;
    public:

        virtual TString GetTaskClassIdentifier() const override {
            return "PlainReading::TAssembleFilter";
        }

        TAssembleFilter(const NActors::TActorId& scanActorId, TPortionInfo::TPreparedBatchData&& batchConstructor, NOlap::TReadMetadata::TConstPtr readMetadata,
            const ui32 sourceIdx, const std::set<ui32>& filterColumnIds, const bool useFilter)
            : TBase(scanActorId)
            , BatchConstructor(batchConstructor)
            , SourceIdx(sourceIdx)
            , ReadMetadata(readMetadata)
            , FilterColumnIds(filterColumnIds)
            , UseFilter(useFilter)
        {
            TBase::SetPriority(TBase::EPriority::Normal);
        }
    };

}
