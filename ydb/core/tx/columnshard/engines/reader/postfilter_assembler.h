#pragma once
#include "common.h"
#include "conveyor_task.h"

#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {
class TBatch;
class TAssembleBatch: public NColumnShard::IDataTasksProcessor::ITask, public NColumnShard::TMonitoringObjectsCounter<TAssembleBatch, true, true> {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    TPortionInfo::TPreparedBatchData BatchConstructor;
    std::shared_ptr<arrow::RecordBatch> FullBatch;
    std::vector<std::string> FullColumnsOrder;

    std::shared_ptr<NArrow::TColumnFilter> Filter;
    std::shared_ptr<arrow::RecordBatch> FilterBatch;

    const TBatchAddress BatchAddress;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
    virtual bool DoExecuteImpl() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Reading::TAssembleBatch";
    }

    TAssembleBatch(TPortionInfo::TPreparedBatchData&& batchConstructor,
        TBatch& currentBatch, const std::vector<std::string>& fullColumnsOrder, NColumnShard::IDataTasksProcessor::TPtr processor);
};
}
