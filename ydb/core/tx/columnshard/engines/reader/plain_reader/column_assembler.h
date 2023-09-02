#pragma once
#include "source.h"
#include <ydb/core/tx/columnshard/engines/reader/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NOlap::NPlainReader {
class TBatch;
class TAssembleBatch: public NColumnShard::IDataTasksProcessor::ITask, public NColumnShard::TMonitoringObjectsCounter<TAssembleBatch, true, true> {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    TPortionInfo::TPreparedBatchData BatchConstructor;
    std::shared_ptr<NArrow::TColumnFilter> Filter;
protected:
    std::shared_ptr<arrow::RecordBatch> Result;
    const ui32 SourceIdx;
    virtual bool DoExecuteImpl() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "PlainReader::TAssembleBatch";
    }

    TAssembleBatch(TPortionInfo::TPreparedBatchData&& batchConstructor,
        const ui32 sourceIdx, const std::shared_ptr<NArrow::TColumnFilter>& filter, const NColumnShard::IDataTasksProcessor::TPtr& processor);
};

class TAssemblePKBatch: public TAssembleBatch {
private:
    using TBase = TAssembleBatch;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
public:
    using TBase::TBase;
};

class TAssembleFFBatch: public TAssembleBatch {
private:
    using TBase = TAssembleBatch;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
public:
    using TBase::TBase;
};
}
