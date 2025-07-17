#include "plain_read_data.h"

#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

namespace NKikimr::NOlap::NReader::NPlain {

TPlainReadData::TPlainReadData(const std::shared_ptr<TReadContext>& context)
    : TBase(context)
    , SpecialReadContext(std::make_shared<TSpecialReadContext>(context)) {
    std::deque<std::shared_ptr<IDataSource>> sources;
    const auto readMetadata = GetReadMetadataVerifiedAs<const TReadMetadata>();
    auto constructor = GetReadMetadata()->ExtractSelectInfo();
    constructor->FillReadStats(GetReadMetadata()->ReadStats);
    Scanner = std::make_shared<TScanHead>(std::move(constructor), SpecialReadContext);
    auto& stats = GetReadMetadata()->ReadStats;
    stats->SchemaColumns = (*SpecialReadContext->GetProgramInputColumns() - *SpecialReadContext->GetSpecColumns()).GetColumnsCount();
}

std::vector<std::unique_ptr<TPartialReadResult>> TPlainReadData::DoExtractReadyResults(const int64_t /*maxRowsInBatch*/) {
    auto result = std::move(PartialResults);
    PartialResults.clear();
    //    auto result = TPartialReadResult::SplitResults(std::move(PartialResults), maxRowsInBatch);
    ui32 count = 0;
    for (auto&& r : result) {
        count += r->GetRecordsCount();
    }
    AFL_VERIFY(count == ReadyResultsCount);
    ReadyResultsCount = 0;

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractReadyResults")("result", result.size())("count", count)(
        "finished", Scanner->IsFinished());
    return result;
}

TConclusion<bool> TPlainReadData::DoReadNextInterval() {
    return Scanner->BuildNextInterval();
}

void TPlainReadData::OnIntervalResult(std::unique_ptr<TPartialReadResult>&& result) {
    //    result->GetResourcesGuardOnly()->Update(result->GetMemorySize());
    ReadyResultsCount += result->GetRecordsCount();
    PartialResults.emplace_back(result);
}

}   // namespace NKikimr::NOlap::NReader::NPlain
