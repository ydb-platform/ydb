#pragma once
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/formats/arrow/permutations.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

namespace NKikimr::NOlap::NReader {

class TReadContext;

class TPartialSourceAddress {
private:
    YDB_READONLY(ui32, SourceId, 0);
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY(ui32, SyncPointIndex, 0);

public:
    TPartialSourceAddress(const ui32 sourceId, const ui32 sourceIdx, const ui32 syncPointIndex)
        : SourceId(sourceId)
        , SourceIdx(sourceIdx)
        , SyncPointIndex(syncPointIndex) {
    }
};

// Represents a batch of rows produced by ASC or DESC scan with applied filters and partial aggregation
class TPartialReadResult: public TNonCopyable {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>, ResourceGuards);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TGroupGuard>, GroupGuard);
    NArrow::TShardedRecordBatch ResultBatch;

    // This 1-row batch contains the last key that was read while producing the ResultBatch.
    // NOTE: it might be different from the Key of last row in ResulBatch in case of filtering/aggregation/limit
    std::shared_ptr<IScanCursor> ScanCursor;
    YDB_READONLY_DEF(std::optional<TPartialSourceAddress>, NotFinishedInterval);
    const NColumnShard::TCounterGuard Guard;
    bool Extracted = false;

public:
    void Cut(const ui32 limit) {
        AFL_VERIFY(!Extracted);
        ResultBatch.Cut(limit);
    }

    const std::shared_ptr<arrow::Table>& GetResultBatch() const {
        AFL_VERIFY(!Extracted);
        return ResultBatch;
    }

    ui64 GetRecordsCount() const {
        AFL_VERIFY(!Extracted);
        return ResultBatch.GetRecordsCount();
    }

    std::shared_ptr<arrow::Schema> GetResultSchema() const {
        AFL_VERIFY(!Extracted);
        return ResultBatch.GetResultSchema();
    }

    static std::vector<std::shared_ptr<TPartialReadResult>> SplitResults(
        std::vector<std::shared_ptr<TPartialReadResult>>&& resultsExt, const ui32 maxRecordsInResult);

    NArrow::TShardedRecordBatch ExtractShardedBatch() {
        AFL_VERIFY(!Extracted);
        Extracted = true;
        return std::move(ResultBatch);
    }

    const std::shared_ptr<IScanCursor>& GetScanCursor() const {
        return ScanCursor;
    }

    explicit TPartialReadResult(std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>&& resourceGuards,
        std::shared_ptr<NGroupedMemoryManager::TGroupGuard>&& gGuard, NArrow::TShardedRecordBatch&& batch,
        std::shared_ptr<IScanCursor>&& scanCursor, const std::shared_ptr<TReadContext>& context,
        const std::optional<TPartialSourceAddress> notFinishedInterval);

    explicit TPartialReadResult(NArrow::TShardedRecordBatch&& batch, std::shared_ptr<IScanCursor>&& scanCursor,
        const std::shared_ptr<TReadContext>& context, const std::optional<TPartialSourceAddress> notFinishedInterval)
        : TPartialReadResult({}, nullptr, std::move(batch), std::move(scanCursor), context, notFinishedInterval) {
    }
};

}   // namespace NKikimr::NOlap::NReader
