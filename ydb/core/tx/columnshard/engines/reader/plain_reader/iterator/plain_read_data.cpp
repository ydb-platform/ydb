#include "plain_read_data.h"

namespace NKikimr::NOlap::NReader::NPlain {

TPlainReadData::TPlainReadData(const std::shared_ptr<TReadContext>& context)
    : TBase(context)
    , SpecialReadContext(std::make_shared<TSpecialReadContext>(context))
{
    ui32 sourceIdx = 0;
    std::deque<std::shared_ptr<IDataSource>> sources;
    const auto& portions = GetReadMetadata()->SelectInfo->PortionsOrderedPK;
    const auto& committed = GetReadMetadata()->CommittedBlobs;
    ui64 compactedPortionsBytes = 0;
    ui64 insertedPortionsBytes = 0;
    ui64 committedPortionsBytes = 0;
    for (auto&& i : portions) {
        if (i->GetMeta().GetProduced() == NPortion::EProduced::COMPACTED || i->GetMeta().GetProduced() == NPortion::EProduced::SPLIT_COMPACTED) {
            compactedPortionsBytes += i->GetTotalBlobBytes();
        } else {
            insertedPortionsBytes += i->GetTotalBlobBytes();
        }
        sources.emplace_back(std::make_shared<TPortionDataSource>(sourceIdx++, i, SpecialReadContext));
    }
    for (auto&& i : committed) {
        if (i.IsCommitted()) {
            continue;
        }
        if (GetReadMetadata()->IsMyUncommitted(i.GetInsertWriteId())) {
            continue;
        }
        if (GetReadMetadata()->GetPKRangesFilter().CheckPoint(i.GetFirst()) ||
            GetReadMetadata()->GetPKRangesFilter().CheckPoint(i.GetLast())) {
            GetReadMetadata()->SetConflictedWriteId(i.GetInsertWriteId());
        }
    }

    for (auto&& i : committed) {
        if (!i.IsCommitted()) {
            if (GetReadMetadata()->IsWriteConflictable(i.GetInsertWriteId())) {
                continue;
            }
        } else if (GetReadMetadata()->GetPKRangesFilter().IsPortionInPartialUsage(i.GetFirst(), i.GetLast()) ==
                   TPKRangeFilter::EUsageClass::DontUsage) {
            continue;
        }
        sources.emplace_back(std::make_shared<TCommittedDataSource>(sourceIdx++, i, SpecialReadContext));
        committedPortionsBytes += i.GetSize();
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

std::vector<std::shared_ptr<TPartialReadResult>> TPlainReadData::DoExtractReadyResults(const int64_t /*maxRowsInBatch*/) {
    auto result = std::move(PartialResults);
    PartialResults.clear();
//    auto result = TPartialReadResult::SplitResults(std::move(PartialResults), maxRowsInBatch);
    ui32 count = 0;
    for (auto&& r: result) {
        count += r->GetRecordsCount();
    }
    AFL_VERIFY(count == ReadyResultsCount);
    ReadyResultsCount = 0;

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExtractReadyResults")("result", result.size())("count", count)("finished", Scanner->IsFinished());
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

}
