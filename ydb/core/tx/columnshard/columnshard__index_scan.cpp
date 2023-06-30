#include "columnshard__index_scan.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor/usage/events.h>

namespace NKikimr::NColumnShard {

TColumnShardScanIterator::TColumnShardScanIterator(NOlap::TReadMetadata::TConstPtr readMetadata, const NOlap::TReadContext& context)
    : Context(context)
    , ReadMetadata(readMetadata)
    , IndexedData(ReadMetadata, false, context)
{
    IndexedData.InitRead();
    Y_VERIFY(ReadMetadata->IsSorted());

    if (ReadMetadata->Empty()) {
        IndexedData.Abort();
    }
}

void TColumnShardScanIterator::AddData(const TBlobRange& blobRange, TString data) {
    IndexedData.AddData(blobRange, data);
}

NKikimr::NOlap::TPartialReadResult TColumnShardScanIterator::GetBatch() {
    FillReadyResults();

    if (ReadyResults.empty()) {
        return {};
    }

    auto result(std::move(ReadyResults.front()));
    ReadyResults.pop_front();

    return result;
}

NKikimr::NColumnShard::TBlobRange TColumnShardScanIterator::GetNextBlobToRead() {
    return IndexedData.ExtractNextBlob();
}

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData.GetReadyResults(MaxRowsInBatch);
    i64 limitLeft = ReadMetadata->Limit == 0 ? INT64_MAX : ReadMetadata->Limit - ItemsRead;
    for (size_t i = 0; i < ready.size() && limitLeft; ++i) {
        if (ready[i].ResultBatch->num_rows() == 0 && !ready[i].LastReadKey) {
            Y_VERIFY(i + 1 == ready.size(), "Only last batch can be empty!");
            break;
        }

        ReadyResults.emplace_back(std::move(ready[i]));
        auto& batch = ReadyResults.back();
        if (batch.ResultBatch->num_rows() > limitLeft) {
            // Trim the last batch if total row count exceeds the requested limit
            batch.ResultBatch = batch.ResultBatch->Slice(0, limitLeft);
        }
        limitLeft -= batch.ResultBatch->num_rows();
        ItemsRead += batch.ResultBatch->num_rows();
    }

    if (limitLeft == 0) {
        IndexedData.Abort();
    }

    if (IndexedData.IsFinished()) {
        Context.MutableProcessor().Stop();
    }
}

bool TColumnShardScanIterator::HasWaitingTasks() const {
    return Context.GetProcessor().InWaiting();
}

TColumnShardScanIterator::~TColumnShardScanIterator() {
    IndexedData.Abort();
    ReadMetadata->ReadStats->PrintToLog();
}

void TColumnShardScanIterator::Apply(IDataTasksProcessor::ITask::TPtr task) {
    if (!task->IsDataProcessed() || Context.GetProcessor().IsStopped() || !task->IsSameProcessor(Context.GetProcessor())) {
        return;
    }
    Y_VERIFY(task->Apply(IndexedData.GetGranulesContext()));
}

}
