#include "result.h"

namespace NKikimr::NOlap::NReader {

class TCurrentBatch {
private:
    std::vector<TPartialReadResult> Results;
    ui64 RecordsCount = 0;
public:
    ui64 GetRecordsCount() const {
        return RecordsCount;
    }

    void AddChunk(TPartialReadResult&& res) {
        RecordsCount += res.GetRecordsCount();
        Results.emplace_back(std::move(res));
    }

    void FillResult(std::vector<TPartialReadResult>& result) const {
        if (Results.empty()) {
            return;
        }
        for (auto&& i : Results) {
            result.emplace_back(std::move(i));
        }
    }
};

std::vector<TPartialReadResult> TPartialReadResult::SplitResults(std::vector<TPartialReadResult>&& resultsExt, const ui32 maxRecordsInResult) {
    std::vector<TCurrentBatch> resultBatches;
    TCurrentBatch currentBatch;
    for (auto&& i : resultsExt) {
        AFL_VERIFY(i.GetRecordsCount());
        currentBatch.AddChunk(std::move(i));
        if (currentBatch.GetRecordsCount() >= maxRecordsInResult) {
            resultBatches.emplace_back(std::move(currentBatch));
            currentBatch = TCurrentBatch();
        }
    }
    if (currentBatch.GetRecordsCount()) {
        resultBatches.emplace_back(std::move(currentBatch));
    }

    std::vector<TPartialReadResult> result;
    for (auto&& i : resultBatches) {
        i.FillResult(result);
    }
    return result;
}

}