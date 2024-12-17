#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

namespace NKikimr::NOlap::NReader {

class TReadContext;

// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
class TPartialReadResult: public TNonCopyable {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>, ResourceGuards);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TGroupGuard>, GroupGuard);
    NArrow::TShardedRecordBatch ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<IScanCursor> ScanCursor;
    YDB_READONLY_DEF(std::optional<ui32>, NotFinishedIntervalIdx);
    const NColumnShard::TCounterGuard Guard;

public:
    void Cut(const ui32 limit) {
        ResultBatch.Cut(limit);
    }

    const arrow::Table& GetResultBatch() const {
        return *ResultBatch.GetRecordBatch();
    }

    const std::shared_ptr<arrow::Table>& GetResultBatchPtrVerified() const {
        AFL_VERIFY(ResultBatch.GetRecordBatch());
        return ResultBatch.GetRecordBatch();
    }

    ui64 GetMemorySize() const {
        return ResultBatch.GetMemorySize();
    }

    ui64 GetRecordsCount() const {
        return ResultBatch.GetRecordsCount();
    }

    static std::vector<std::shared_ptr<TPartialReadResult>> SplitResults(
        std::vector<std::shared_ptr<TPartialReadResult>>&& resultsExt, const ui32 maxRecordsInResult);

    const NArrow::TShardedRecordBatch& GetShardedBatch() const {
        return ResultBatch;
    }

    const std::shared_ptr<IScanCursor>& GetScanCursor() const {
        return ScanCursor;
    }

    explicit TPartialReadResult(const std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>& resourceGuards,
        const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& gGuard, const NArrow::TShardedRecordBatch& batch,
        const std::shared_ptr<IScanCursor>& scanCursor, const std::shared_ptr<TReadContext>& context,
        const std::optional<ui32> notFinishedIntervalIdx);

    explicit TPartialReadResult(const NArrow::TShardedRecordBatch& batch, const std::shared_ptr<IScanCursor>& scanCursor,
        const std::shared_ptr<TReadContext>& context, const std::optional<ui32> notFinishedIntervalIdx)
        : TPartialReadResult({}, nullptr, batch, scanCursor, context, notFinishedIntervalIdx) {
    }
};

}   // namespace NKikimr::NOlap::NReader
