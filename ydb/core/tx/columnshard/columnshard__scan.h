#pragma once

#include "blob_cache.h"
#include "engines/reader/conveyor_task.h"
#include "resources/memory.h"
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/program/program.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
class TPartialReadResult {
private:
    std::shared_ptr<TScanMemoryLimiter::TGuard> MemoryGuard;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<arrow::RecordBatch> LastReadKey;

public:
    void Slice(const ui32 offset, const ui32 length) {
        const ui64 baseSize = NArrow::GetBatchDataSize(ResultBatch);
        ResultBatch = ResultBatch->Slice(offset, length);
        MemoryGuard->Take(NArrow::GetBatchDataSize(ResultBatch));
        MemoryGuard->Free(baseSize);
    }

    void ApplyProgram(const NOlap::TProgramContainer& program) {
        const ui64 baseSize = NArrow::GetBatchDataSize(ResultBatch);
        auto status = program.ApplyProgram(ResultBatch);
        if (!status.ok()) {
            ErrorString = status.message();
        } else {
            MemoryGuard->Take(NArrow::GetBatchDataSize(ResultBatch));
            MemoryGuard->Free(baseSize);
        }
    }

    ui64 GetSize() const {
        if (MemoryGuard) {
            return MemoryGuard->GetValue();
        } else {
            return 0;
        }
    }

    const std::shared_ptr<arrow::RecordBatch>& GetResultBatch() const {
        return ResultBatch;
    }

    const std::shared_ptr<arrow::RecordBatch>& GetLastReadKey() const {
        return LastReadKey;
    }

    std::string ErrorString;

    TPartialReadResult() = default;

    explicit TPartialReadResult(
        TScanMemoryLimiter::IMemoryAccessor::TPtr memoryAccessor,
        const std::shared_ptr<NOlap::TMemoryAggregation>& memoryAggregation,
        std::shared_ptr<arrow::RecordBatch> batch)
        : MemoryGuard(std::make_shared<TScanMemoryLimiter::TGuard>(memoryAccessor, memoryAggregation))
        , ResultBatch(batch)
    {
        MemoryGuard->Take(NArrow::GetBatchDataSize(ResultBatch));
        MemoryGuard->Take(NArrow::GetBatchDataSize(LastReadKey));
    }

    explicit TPartialReadResult(
        TScanMemoryLimiter::IMemoryAccessor::TPtr memoryAccessor,
        const std::shared_ptr<NOlap::TMemoryAggregation>& memoryAggregation,
        std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<arrow::RecordBatch> lastKey)
        : MemoryGuard(std::make_shared<TScanMemoryLimiter::TGuard>(memoryAccessor, memoryAggregation))
        , ResultBatch(batch)
        , LastReadKey(lastKey)
    {
        MemoryGuard->Take(NArrow::GetBatchDataSize(ResultBatch));
        MemoryGuard->Take(NArrow::GetBatchDataSize(LastReadKey));
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
