#include "plain_read_data.h"

namespace NKikimr::NOlap::NPlainReader {

TPlainReadData::TPlainReadData(const std::shared_ptr<NOlap::TReadContext>& context)
    : TBase(context)
    , SpecialReadContext(std::make_shared<TSpecialReadContext>(context))
{
    ui32 sourceIdx = 0;
    std::deque<std::shared_ptr<IDataSource>> sources;
    const auto& portionsOrdered = GetReadMetadata()->SelectInfo->GetPortionsOrdered(GetReadMetadata()->IsDescSorted());
    const auto& committed = GetReadMetadata()->CommittedBlobs;
    auto itCommitted = committed.begin();
    auto itPortion = portionsOrdered.begin();
    ui64 committedPortionsBytes = 0;
    ui64 insertedPortionsBytes = 0;
    ui64 compactedPortionsBytes = 0;
    while (itCommitted != committed.end() || itPortion != portionsOrdered.end()) {
        bool movePortion = false;
        if (itCommitted == committed.end()) {
            movePortion = true;
        } else if (itPortion == portionsOrdered.end()) {
            movePortion = false;
        } else if (itCommitted->GetFirstVerified() < (*itPortion)->IndexKeyStart()) {
            movePortion = false;
        } else {
            movePortion = true;
        }

        if (movePortion) {
            if ((*itPortion)->GetMeta().GetProduced() == NPortion::EProduced::COMPACTED || (*itPortion)->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
                compactedPortionsBytes += (*itPortion)->BlobsBytes();
            } else {
                insertedPortionsBytes += (*itPortion)->BlobsBytes();
            }
            sources.emplace_back(std::make_shared<TPortionDataSource>(sourceIdx++, *itPortion, SpecialReadContext, (*itPortion)->IndexKeyStart(), (*itPortion)->IndexKeyEnd()));
            ++itPortion;
        } else {
            sources.emplace_back(std::make_shared<TCommittedDataSource>(sourceIdx++, *itCommitted, SpecialReadContext, itCommitted->GetFirstVerified(), itCommitted->GetLastVerified()));
            committedPortionsBytes += itCommitted->GetSize();
            ++itCommitted;
        }
    }
    Scanner = std::make_shared<TScanHead>(std::move(sources), SpecialReadContext);

    auto& stats = GetReadMetadata()->ReadStats;
    stats->IndexPortions = GetReadMetadata()->SelectInfo->PortionsOrderedPK.size();
    stats->IndexBatches = GetReadMetadata()->NumIndexedBlobs();
    stats->CommittedBatches = GetReadMetadata()->CommittedBlobs.size();
    stats->SchemaColumns = (*SpecialReadContext->GetProgramInputColumns() - *SpecialReadContext->GetSpecColumns()).GetColumnsCount();
    stats->CommittedPortionsBytes = committedPortionsBytes;
    stats->InsertedPortionsBytes = insertedPortionsBytes;
    stats->CompactedPortionsBytes = compactedPortionsBytes;

}

std::vector<NKikimr::NOlap::TPartialReadResult> TPlainReadData::DoExtractReadyResults(const int64_t maxRowsInBatch) {
    auto result = TPartialReadResult::SplitResults(std::move(PartialResults), maxRowsInBatch);
    ui32 count = 0;
    for (auto&& r: result) {
        count += r.GetRecordsCount();
    }
    AFL_VERIFY(count == ReadyResultsCount);

    ReadyResultsCount = 0;
    PartialResults.clear();

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractReadyResults")("result", result.size())("count", count)("finished", Scanner->IsFinished());
    return result;
}

bool TPlainReadData::DoReadNextInterval() {
    return Scanner->BuildNextInterval();
}

void TPlainReadData::OnIntervalResult(const std::shared_ptr<TPartialReadResult>& result) {
    result->GetResourcesGuardOnly()->Update(result->GetMemorySize());
    ReadyResultsCount += result->GetRecordsCount();
    PartialResults.emplace_back(std::move(*result));
}

}
