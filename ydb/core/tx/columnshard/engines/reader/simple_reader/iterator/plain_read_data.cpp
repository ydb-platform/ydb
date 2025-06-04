#include "plain_read_data.h"

#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

namespace NKikimr::NOlap::NReader::NSimple {

TPlainReadData::TPlainReadData(const std::shared_ptr<TReadContext>& context)
    : TBase(context)
    , SpecialReadContext(std::make_shared<TSpecialReadContext>(context)) {
    ui32 sourceIdx = 0;
    std::deque<TSourceConstructor> sources;
    const auto& portions = GetReadMetadata()->SelectInfo->Portions;
    ui64 compactedPortionsBytes = 0;
    ui64 insertedPortionsBytes = 0;
    for (auto&& i : portions) {
        if (i->GetPortionType() == EPortionType::Compacted) {
            compactedPortionsBytes += i->GetTotalBlobBytes();
        } else {
            insertedPortionsBytes += i->GetTotalBlobBytes();
        }

        sources.emplace_back(TSourceConstructor(sourceIdx++, i, context));
    }
    Scanner = std::make_shared<TScanHead>(std::move(sources), SpecialReadContext);

    auto& stats = GetReadMetadata()->ReadStats;
    stats->IndexPortions = GetReadMetadata()->SelectInfo->Portions.size();
    stats->IndexBatches = GetReadMetadata()->NumIndexedBlobs();
    stats->SchemaColumns = (*SpecialReadContext->GetProgramInputColumns() - *SpecialReadContext->GetSpecColumns()).GetColumnsCount();
    stats->InsertedPortionsBytes = insertedPortionsBytes;
    stats->CompactedPortionsBytes = compactedPortionsBytes;
}

std::vector<std::shared_ptr<TPartialReadResult>> TPlainReadData::DoExtractReadyResults(const int64_t /*maxRowsInBatch*/) {
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

void TPlainReadData::OnIntervalResult(const std::shared_ptr<TPartialReadResult>& result) {
    //    result->GetResourcesGuardOnly()->Update(result->GetMemorySize());
    ReadyResultsCount += result->GetRecordsCount();
    PartialResults.emplace_back(result);
}

void TPlainReadData::OnSentDataFromInterval(const TPartialSourceAddress& sourceAddress) {
    if (!SpecialReadContext->IsActive()) {
        return;
    }
    Scanner->GetSyncPoint(sourceAddress.GetSyncPointIndex())->Continue(sourceAddress, *this);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
