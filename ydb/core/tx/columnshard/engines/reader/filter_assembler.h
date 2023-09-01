#pragma once
#include "common.h"
#include "conveyor_task.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

    class TAssembleFilter: public NColumnShard::IDataTasksProcessor::ITask, public NColumnShard::TMonitoringObjectsCounter<TAssembleFilter, true, true> {
    private:
        using TBase = NColumnShard::IDataTasksProcessor::ITask;
        TPortionInfo::TPreparedBatchData BatchConstructor;
        std::shared_ptr<arrow::RecordBatch> FilteredBatch;
        NOlap::TReadMetadata::TConstPtr ReadMetadata;
        std::shared_ptr<NArrow::TColumnFilter> Filter;
        std::shared_ptr<NArrow::TColumnFilter> EarlyFilter;
        const TBatchAddress BatchAddress;
        ui32 OriginalCount = 0;
        bool AllowEarlyFilter = false;
        std::set<ui32> FilterColumnIds;
        IOrderPolicy::TPtr BatchesOrderPolicy;
    protected:
        virtual bool DoApply(IDataReader& owner) const override;
        virtual bool DoExecuteImpl() override;
    public:

        virtual TString GetTaskClassIdentifier() const override {
            return "Reading::TAssembleFilter";
        }

        TAssembleFilter(TPortionInfo::TPreparedBatchData&& batchConstructor, NOlap::TReadMetadata::TConstPtr readMetadata,
            TBatch& batch, const std::set<ui32>& filterColumnIds, NColumnShard::IDataTasksProcessor::TPtr processor,
            IOrderPolicy::TPtr batchesOrderPolicy)
            : TBase(processor)
            , BatchConstructor(batchConstructor)
            , ReadMetadata(readMetadata)
            , BatchAddress(batch.GetBatchAddress())
            , AllowEarlyFilter(batch.AllowEarlyFilter())
            , FilterColumnIds(filterColumnIds)
            , BatchesOrderPolicy(batchesOrderPolicy)
        {
            TBase::SetPriority(TBase::EPriority::Normal);
        }
    };

}
