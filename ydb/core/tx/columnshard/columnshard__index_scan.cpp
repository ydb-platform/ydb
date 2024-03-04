#include "columnshard__index_scan.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor/usage/events.h>

namespace NKikimr::NColumnShard {

TColumnShardScanIterator::TColumnShardScanIterator(const std::shared_ptr<NOlap::TReadContext>& context, const NOlap::TReadMetadata::TConstPtr& readMetadata)
    : Context(context)
    , ReadMetadata(readMetadata)
    , ReadyResults(context->GetCounters())
{
    IndexedData = readMetadata->BuildReader(Context);
    Y_ABORT_UNLESS(Context->GetReadMetadata()->IsSorted());

    if (readMetadata->Empty()) {
        IndexedData->Abort();
    }
}

std::optional<NOlap::TPartialReadResult> TColumnShardScanIterator::GetBatch() {
    FillReadyResults();
    return ReadyResults.pop_front();
}

void TColumnShardScanIterator::PrepareResults() {
    FillReadyResults();
}

bool TColumnShardScanIterator::ReadNextInterval() {
    return IndexedData->ReadNextInterval();
}

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData->ExtractReadyResults(MaxRowsInBatch);
    i64 limitLeft = Context->GetReadMetadata()->Limit == 0 ? INT64_MAX : Context->GetReadMetadata()->Limit - ItemsRead;
    for (size_t i = 0; i < ready.size() && limitLeft; ++i) {
        if (ready[i].GetResultBatch().num_rows() == 0 && !ready[i].GetLastReadKey()) {
            Y_ABORT_UNLESS(i + 1 == ready.size(), "Only last batch can be empty!");
            break;
        }

        auto& batch = ReadyResults.emplace_back(std::move(ready[i]));
        if (batch.GetResultBatch().num_rows() > limitLeft) {
            batch.Slice(0, limitLeft);
        }
        limitLeft -= batch.GetResultBatch().num_rows();
        ItemsRead += batch.GetResultBatch().num_rows();
    }

    if (limitLeft == 0) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort_scan")("limit", Context->GetReadMetadata()->Limit)("ready", ItemsRead);
        IndexedData->Abort();
    }
}

TColumnShardScanIterator::~TColumnShardScanIterator() {
    IndexedData->Abort();
    ReadMetadata->ReadStats->PrintToLog();
}

void TColumnShardScanIterator::Apply(IDataTasksProcessor::ITask::TPtr task) {
    if (!IndexedData->IsFinished()) {
        Y_ABORT_UNLESS(task->Apply(*IndexedData));
    }
}

}
