#include "iterator.h"

namespace NKikimr::NOlap::NReader::NPlain {

TColumnShardScanIterator::TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context, const TReadMetadata::TConstPtr& readMetadata)
    : Context(context)
    , ReadMetadata(readMetadata)
    , ReadyResults(context->GetCounters())
{
    IndexedData = readMetadata->BuildReader(Context);
    Y_ABORT_UNLESS(Context->GetReadMetadata()->IsSorted());
}

TConclusion<std::shared_ptr<TPartialReadResult>> TColumnShardScanIterator::GetBatch() {
    FillReadyResults();
    return ReadyResults.pop_front();
}

void TColumnShardScanIterator::PrepareResults() {
    FillReadyResults();
}

TConclusion<bool> TColumnShardScanIterator::ReadNextInterval() {
    return IndexedData->ReadNextInterval();
}

void TColumnShardScanIterator::DoOnSentDataFromInterval(const ui32 intervalIdx) const {
    return IndexedData->OnSentDataFromInterval(intervalIdx);
}

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData->ExtractReadyResults(MaxRowsInBatch);
    i64 limitLeft = Context->GetReadMetadata()->Limit == 0 ? INT64_MAX : Context->GetReadMetadata()->Limit - ItemsRead;
    for (size_t i = 0; i < ready.size() && limitLeft; ++i) {
        auto& batch = ReadyResults.emplace_back(std::move(ready[i]));
        if (batch->GetResultBatch().num_rows() > limitLeft) {
            batch->Cut(limitLeft);
        }
        limitLeft -= batch->GetResultBatch().num_rows();
        ItemsRead += batch->GetResultBatch().num_rows();
    }

    if (limitLeft == 0) {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "limit_reached_on_scan")("limit", Context->GetReadMetadata()->Limit)("ready", ItemsRead);
        IndexedData->Abort("records count limit exhausted");
    }
}

TColumnShardScanIterator::~TColumnShardScanIterator() {
    if (!IndexedData->IsFinished()) {
        IndexedData->Abort("iterator destructor");
    }
    ReadMetadata->ReadStats->PrintToLog();
}

void TColumnShardScanIterator::Apply(const std::shared_ptr<IApplyAction>& task) {
    if (!IndexedData->IsFinished()) {
        Y_ABORT_UNLESS(task->Apply(*IndexedData));
    }
}

}
