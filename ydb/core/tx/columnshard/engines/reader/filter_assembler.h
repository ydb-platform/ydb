#pragma once
#include "conveyor_task.h"

#include <ydb/core/formats/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

    class TAssembleFilter: public NColumnShard::IDataTasksProcessor::ITask {
    private:
        using TBase = NColumnShard::IDataTasksProcessor::ITask;
        TPortionInfo::TPreparedBatchData BatchConstructor;
        std::shared_ptr<arrow::RecordBatch> FilteredBatch;
        NOlap::TReadMetadata::TConstPtr ReadMetadata;
        std::shared_ptr<NArrow::TColumnFilter> Filter;
        const ui32 BatchNo;
        bool AllowEarlyFilter = false;
    protected:
        virtual bool DoApply(TIndexedReadData& owner) const override;
        virtual bool DoExecuteImpl() override;
    public:
        TAssembleFilter(TPortionInfo::TPreparedBatchData&& batchConstructor, NOlap::TReadMetadata::TConstPtr readMetadata,
            TBatch& batch, const bool allowEarlyFilter, NColumnShard::IDataTasksProcessor::TPtr processor)
            : TBase(processor)
            , BatchConstructor(batchConstructor)
            , ReadMetadata(readMetadata)
            , BatchNo(batch.GetBatchNo())
            , AllowEarlyFilter(allowEarlyFilter)
        {

        }
    };

}
