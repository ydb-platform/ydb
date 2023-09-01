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
    std::shared_ptr<TScanMemoryLimiter::TGuard> MemoryGuardExternal;
    std::shared_ptr<TScanMemoryLimiter::TGuard> MemoryGuardInternal;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<arrow::RecordBatch> LastReadKey;

public:
    ui64 GetRecordsCount() const {
        return ResultBatch ? ResultBatch->num_rows() : 0;
    }

    void InitDirection(const bool reverse) {
        if (reverse && ResultBatch && ResultBatch->num_rows()) {
            auto permutation = NArrow::MakePermutation(ResultBatch->num_rows(), true);
            ResultBatch = NArrow::TStatusValidator::GetValid(arrow::compute::Take(ResultBatch, permutation)).record_batch();
        }
    }

    void StripColumns(const std::shared_ptr<arrow::Schema>& schema) {
        if (ResultBatch) {
            ResultBatch = NArrow::ExtractColumns(ResultBatch, schema);
        }
    }

    void BuildLastKey(const std::shared_ptr<arrow::Schema>& schema) {
        Y_VERIFY(!LastReadKey);
        if (ResultBatch && ResultBatch->num_rows()) {
            auto keyColumns = NArrow::ExtractColumns(ResultBatch, schema);
            Y_VERIFY(keyColumns);
            LastReadKey = keyColumns->Slice(keyColumns->num_rows() - 1);
        }
    }

    static std::vector<TPartialReadResult> SplitResults(const std::vector<TPartialReadResult>& resultsExt, const ui32 maxRecordsInResult) {
        std::vector<TPartialReadResult> result;
        std::shared_ptr<arrow::RecordBatch> currentBatch;
        for (auto&& i : resultsExt) {
            std::shared_ptr<arrow::RecordBatch> currentBatchSplitting = i.ResultBatch;
            while (currentBatchSplitting && currentBatchSplitting->num_rows()) {
                const ui32 currentRecordsCount = currentBatch ? currentBatch->num_rows() : 0;
                if (currentRecordsCount + currentBatchSplitting->num_rows() < maxRecordsInResult) {
                    if (!currentBatch) {
                        currentBatch = currentBatchSplitting;
                    } else {
                        currentBatch = NArrow::CombineBatches({currentBatch, currentBatchSplitting});
                    }
                    currentBatchSplitting = nullptr;
                } else {
                    auto currentSlice = currentBatchSplitting->Slice(0, maxRecordsInResult - currentRecordsCount);
                    if (!currentBatch) {
                        currentBatch = currentSlice;
                    } else {
                        currentBatch = NArrow::CombineBatches({currentBatch, currentSlice});
                    }
                    result.emplace_back(TPartialReadResult(nullptr, currentBatch));
                    currentBatch = nullptr;
                    currentBatchSplitting = currentBatchSplitting->Slice(maxRecordsInResult - currentRecordsCount);
                }
            }
        }
        if (currentBatch && currentBatch->num_rows()) {
            result.emplace_back(TPartialReadResult(nullptr, currentBatch));
        }
        return result;
    }

    void Slice(const ui32 offset, const ui32 length) {
        ResultBatch = ResultBatch->Slice(offset, length);
    }

    void ApplyProgram(const NOlap::TProgramContainer& program) {
        if (!program.HasProgram()) {
            return;
        }
        Y_VERIFY(!MemoryGuardInternal);
        auto status = program.ApplyProgram(ResultBatch);
        if (!status.ok()) {
            ErrorString = status.message();
        } else if (MemoryGuardExternal) {
            MemoryGuardInternal = MemoryGuardExternal->MakeSame();
            MemoryGuardInternal->Take(NArrow::GetBatchMemorySize(ResultBatch));
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
        std::shared_ptr<TScanMemoryLimiter::TGuard> memGuard,
        std::shared_ptr<arrow::RecordBatch> batch)
        : MemoryGuardExternal(memGuard)
        , ResultBatch(batch)
    {
    }

    explicit TPartialReadResult(
        std::shared_ptr<TScanMemoryLimiter::TGuard> memGuard,
        std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<arrow::RecordBatch> lastKey)
        : MemoryGuardExternal(memGuard)
        , ResultBatch(batch)
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
    virtual std::optional<NBlobCache::TBlobRange> GetNextBlobToRead() { return {}; }
    virtual TString DebugString() const {
        return "NO_DATA";
    }
};

}
