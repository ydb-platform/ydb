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
    ui64 portionsBytes = 0;
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
            portionsBytes += (*itPortion)->BlobsBytes();
            auto start = GetReadMetadata()->BuildSortedPosition((*itPortion)->IndexKeyStart());
            auto finish = GetReadMetadata()->BuildSortedPosition((*itPortion)->IndexKeyEnd());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", start.DebugJson())("finish", finish.DebugJson());
            sources.emplace_back(std::make_shared<TPortionDataSource>(sourceIdx++, *itPortion, SpecialReadContext, start, finish));
            ++itPortion;
        } else {
            auto start = GetReadMetadata()->BuildSortedPosition(itCommitted->GetFirstVerified());
            auto finish = GetReadMetadata()->BuildSortedPosition(itCommitted->GetLastVerified());
            sources.emplace_back(std::make_shared<TCommittedDataSource>(sourceIdx++, *itCommitted, SpecialReadContext, start, finish));
            ++itCommitted;
        }
    }
    Scanner = std::make_shared<TScanHead>(std::move(sources), SpecialReadContext);

    auto& stats = GetReadMetadata()->ReadStats;
    stats->IndexPortions = GetReadMetadata()->SelectInfo->PortionsOrderedPK.size();
    stats->IndexBatches = GetReadMetadata()->NumIndexedBlobs();
    stats->CommittedBatches = GetReadMetadata()->CommittedBlobs.size();
    stats->SchemaColumns = GetReadMetadata()->GetSchemaColumnsCount();
    stats->PortionsBytes = portionsBytes;

}

std::vector<NKikimr::NOlap::TPartialReadResult> TPlainReadData::DoExtractReadyResults(const int64_t maxRowsInBatch) {
    if ((!ReadyResultsCount || (GetContext().GetIsInternalRead() && ReadyResultsCount < maxRowsInBatch)) && !Scanner->IsFinished()) {
        return {};
    }
    ReadyResultsCount = 0;

    auto result = TPartialReadResult::SplitResults(PartialResults, maxRowsInBatch, GetContext().GetIsInternalRead());
    PartialResults.clear();
    ui32 count = 0;
    for (auto&& r: result) {
        r.BuildLastKey(GetReadMetadata()->GetIndexInfo().GetReplaceKey());
        r.StripColumns(GetReadMetadata()->GetResultSchema());
        count += r.GetRecordsCount();
        r.ApplyProgram(GetReadMetadata()->GetProgram());
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractReadyResults")("result", result.size())("count", count)("finished", Scanner->IsFinished());
    return result;
}

bool TPlainReadData::DoReadNextInterval() {
    return Scanner->BuildNextInterval();
}

void TPlainReadData::OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) {
    if (batch && batch->num_rows()) {
        AFL_VERIFY(resourcesGuard);
        resourcesGuard->Update(NArrow::GetBatchMemorySize(batch));
        TPartialReadResult result({resourcesGuard}, batch);
        ReadyResultsCount += result.GetRecordsCount();
        PartialResults.emplace_back(std::move(result));
    }
}

}
