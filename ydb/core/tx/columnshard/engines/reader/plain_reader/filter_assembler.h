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
        const std::shared_ptr<IDataSource> Source;
        TReadMetadata::TConstPtr ReadMetadata;
        std::shared_ptr<NArrow::TColumnFilter> AppliedFilter;
        std::shared_ptr<NArrow::TColumnFilter> EarlyFilter;
        const TSnapshot RecordsMaxSnapshot;
        ui32 OriginalCount = 0;
        bool AllowEarlyFilter = false;
        std::set<ui32> FilterColumnIds;
        const bool UseFilter = true;
        const NColumnShard::TCounterGuard TaskGuard;
    protected:
        virtual bool DoApply(IDataReader& owner) const override;
        virtual bool DoExecute() override;
    public:

        virtual TString GetTaskClassIdentifier() const override {
            return "PlainReading::TAssembleFilter";
        }

        TAssembleFilter(const NActors::TActorId& scanActorId, TPortionInfo::TPreparedBatchData&& batchConstructor, NOlap::TReadMetadata::TConstPtr readMetadata,
            const std::shared_ptr<IDataSource>& source, const std::set<ui32>& filterColumnIds, const bool useFilter, NColumnShard::TCounterGuard&& taskGuard, const TSnapshot& recordsMaxSnapshot)
            : TBase(scanActorId)
            , BatchConstructor(batchConstructor)
            , Source(source)
            , ReadMetadata(readMetadata)
            , RecordsMaxSnapshot(recordsMaxSnapshot)
            , FilterColumnIds(filterColumnIds)
            , UseFilter(useFilter)
            , TaskGuard(std::move(taskGuard))
        {
            Y_UNUSED(RecordsMaxSnapshot);
            TBase::SetPriority(TBase::EPriority::Normal);
        }
    };

}
