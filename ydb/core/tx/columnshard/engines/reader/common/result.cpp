#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader {

class TCurrentBatch {
private:
    std::vector<std::shared_ptr<TPartialReadResult>> Results;
    ui64 RecordsCount = 0;

public:
    ui64 GetRecordsCount() const {
        return RecordsCount;
    }

    void AddChunk(std::shared_ptr<TPartialReadResult>&& res) {
        RecordsCount += res->GetRecordsCount();
        Results.emplace_back(std::move(res));
    }

    void FillResult(std::vector<std::shared_ptr<TPartialReadResult>>& result) const {
        if (Results.empty()) {
            return;
        }
        for (auto&& i : Results) {
            result.emplace_back(std::move(i));
        }
    }
};

std::vector<std::shared_ptr<TPartialReadResult>> TPartialReadResult::SplitResults(
    std::vector<std::shared_ptr<TPartialReadResult>>&& resultsExt, const ui32 maxRecordsInResult) {
    std::vector<TCurrentBatch> resultBatches;
    TCurrentBatch currentBatch;
    for (auto&& i : resultsExt) {
        AFL_VERIFY(i->GetRecordsCount());
        currentBatch.AddChunk(std::move(i));
        if (currentBatch.GetRecordsCount() >= maxRecordsInResult) {
            resultBatches.emplace_back(std::move(currentBatch));
            currentBatch = TCurrentBatch();
        }
    }
    if (currentBatch.GetRecordsCount()) {
        resultBatches.emplace_back(std::move(currentBatch));
    }

    std::vector<std::shared_ptr<TPartialReadResult>> result;
    for (auto&& i : resultBatches) {
        i.FillResult(result);
    }
    return result;
}

TPartialReadResult::TPartialReadResult(const std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>& resourceGuards,
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& gGuard, NArrow::TShardedRecordBatch&& batch,
    std::shared_ptr<IScanCursor>&& scanCursor, const std::shared_ptr<TReadContext>& context,
    const std::optional<TPartialSourceAddress> notFinishedInterval)
    : ResourceGuards(resourceGuards)
    , GroupGuard(gGuard)
    , ResultBatch(std::move(batch))
    , ScanCursor(std::move(scanCursor))
    , NotFinishedInterval(notFinishedInterval)
    , Guard(TValidator::CheckNotNull(context)->GetCounters().GetResultsForReplyGuard()) {
    Y_ABORT_UNLESS(ResultBatch.GetRecordsCount());
    Y_ABORT_UNLESS(ScanCursor);
}

}   // namespace NKikimr::NOlap::NReader
