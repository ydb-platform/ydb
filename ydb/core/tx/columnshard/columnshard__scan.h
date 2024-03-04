#pragma once

#include "blob_cache.h"
#include "blobs_reader/task.h"
#include "engines/reader/conveyor_task.h"
#include "resources/memory.h"
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/program/program.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
class TPartialReadResult {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>>, ResourcesGuards);
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
        Y_ABORT_UNLESS(!LastReadKey);
        if (ResultBatch && ResultBatch->num_rows()) {
            auto keyColumns = NArrow::ExtractColumns(ResultBatch, schema);
            Y_ABORT_UNLESS(keyColumns);
            LastReadKey = keyColumns->Slice(keyColumns->num_rows() - 1);
        }
    }

    static std::vector<TPartialReadResult> SplitResults(const std::vector<TPartialReadResult>& resultsExt, const ui32 maxRecordsInResult, const bool mergePartsToMax);

    void Slice(const ui32 offset, const ui32 length) {
        ResultBatch = ResultBatch->Slice(offset, length);
    }

    void ApplyProgram(const NOlap::TProgramContainer& program) {
        if (!program.HasProgram()) {
            return;
        }
        auto status = program.ApplyProgram(ResultBatch);
        if (!status.ok()) {
            ErrorString = status.message();
        }
    }

    const std::shared_ptr<arrow::RecordBatch>& GetResultBatchPtrVerified() const {
        Y_ABORT_UNLESS(ResultBatch);
        return ResultBatch;
    }

    const arrow::RecordBatch& GetResultBatch() const {
        Y_ABORT_UNLESS(ResultBatch);
        return *ResultBatch;
    }

    const std::shared_ptr<arrow::RecordBatch>& GetLastReadKey() const {
        return LastReadKey;
    }

    std::string ErrorString;

    explicit TPartialReadResult(
        const std::vector<std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>>& resourcesGuards,
        std::shared_ptr<arrow::RecordBatch> batch)
        : ResourcesGuards(resourcesGuards)
        , ResultBatch(batch)
    {
        Y_ABORT_UNLESS(ResultBatch);
    }

    explicit TPartialReadResult(
        const std::vector<std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>>& resourcesGuards,
        std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<arrow::RecordBatch> lastKey)
        : ResourcesGuards(resourcesGuards)
        , ResultBatch(batch)
        , LastReadKey(lastKey)
    {
        Y_ABORT_UNLESS(ResultBatch);
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
    virtual bool Finished() const = 0;
    virtual std::optional<NOlap::TPartialReadResult> GetBatch() = 0;
    virtual void PrepareResults() {

    }
    virtual bool ReadNextInterval() { return false; }
    virtual TString DebugString(const bool verbose = false) const {
        Y_UNUSED(verbose);
        return "NO_DATA";
    }
};

}
