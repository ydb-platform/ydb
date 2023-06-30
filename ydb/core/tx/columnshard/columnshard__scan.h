#pragma once

#include "blob_cache.h"
#include "engines/reader/conveyor_task.h"
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/program/program.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
class TPartialReadResult {
private:
    std::shared_ptr<arrow::RecordBatch> ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<arrow::RecordBatch> LastReadKey;

public:
    void Slice(const ui32 offset, const ui32 length) {
        ResultBatch = ResultBatch->Slice(offset, length);
    }

    void ApplyProgram(const NOlap::TProgramContainer& program) {
        auto status = program.ApplyProgram(ResultBatch);
        if (!status.ok()) {
            ErrorString = status.message();
        }
    }

    ui64 GetSize() const {
        return NArrow::GetBatchDataSize(ResultBatch);
    }

    const std::shared_ptr<arrow::RecordBatch>& GetResultBatch() const {
        return ResultBatch;
    }

    const std::shared_ptr<arrow::RecordBatch>& GetLastReadKey() const {
        return LastReadKey;
    }

    std::string ErrorString;

    TPartialReadResult() = default;

    explicit TPartialReadResult(std::shared_ptr<arrow::RecordBatch> batch)
        : ResultBatch(batch)
    {
    }

    explicit TPartialReadResult(
        std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<arrow::RecordBatch> lastKey)
        : ResultBatch(batch)
        , LastReadKey(lastKey)
    {
    }
};
}

namespace NKikimr::NColumnShard {

class TScanIteratorBase {
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(IDataTasksProcessor::ITask::TPtr /*processor*/) {

    }
    virtual std::optional<ui32> GetAvailableResultsCount() const {
        return {};
    }
    virtual void AddData(const NBlobCache::TBlobRange& /*blobRange*/, TString /*data*/) {}
    virtual bool HasWaitingTasks() const = 0;
    virtual bool Finished() const = 0;
    virtual NOlap::TPartialReadResult GetBatch() = 0;
    virtual NBlobCache::TBlobRange GetNextBlobToRead() { return NBlobCache::TBlobRange(); }
    virtual TString DebugString() const {
        return "NO_DATA";
    }
};

}
