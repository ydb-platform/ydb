#include "columnshard__index_scan.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor/usage/events.h>

namespace NKikimr::NColumnShard {

TColumnShardScanIterator::TColumnShardScanIterator(NOlap::TReadMetadata::TConstPtr readMetadata,
    NColumnShard::TDataTasksProcessorContainer processor, const NColumnShard::TScanCounters& scanCounters)
    : ReadMetadata(readMetadata)
    , IndexedData(ReadMetadata, FetchBlobsQueue, false, scanCounters)
    , DataTasksProcessor(processor)
    , ScanCounters(scanCounters)
{
    ui32 batchNo = 0;
    for (size_t i = 0; i < ReadMetadata->CommittedBlobs.size(); ++i, ++batchNo) {
        const auto& cmtBlob = ReadMetadata->CommittedBlobs[i];
        WaitCommitted.emplace(cmtBlob, batchNo);
    }
    // Read all committed blobs
    for (const auto& cmtBlob : ReadMetadata->CommittedBlobs) {
        auto& blobId = cmtBlob.BlobId;
        FetchBlobsQueue.emplace_back(TBlobRange(blobId, 0, blobId.BlobSize()));
    }
    IndexedData.InitRead(batchNo, true);
    // Add cached batches without read
    for (auto& [blobId, batch] : ReadMetadata->CommittedBatches) {
        auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{ blobId, 0, 0 });
        Y_VERIFY(!cmt.empty());

        const NOlap::TCommittedBlob& cmtBlob = cmt.key();
        ui32 batchNo = cmt.mapped();
        IndexedData.AddNotIndexed(batchNo, batch, cmtBlob.PlanStep, cmtBlob.TxId);
    }

    Y_VERIFY(ReadMetadata->IsSorted());

    if (ReadMetadata->Empty()) {
        FetchBlobsQueue.Stop();
    }
}

void TColumnShardScanIterator::AddData(const TBlobRange& blobRange, TString data) {
    const auto& blobId = blobRange.BlobId;
    if (IndexedData.IsIndexedBlob(blobRange)) {
        IndexedData.AddIndexed(blobRange, data, DataTasksProcessor);
    } else {
        auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{ blobId, 0, 0 });
        if (cmt.empty()) {
            return; // ignore duplicates
        }
        const NOlap::TCommittedBlob& cmtBlob = cmt.key();
        ui32 batchNo = cmt.mapped();
        IndexedData.AddNotIndexed(batchNo, data, cmtBlob.PlanStep, cmtBlob.TxId);
    }
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
    return FetchBlobsQueue.pop_front();
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
            ready.clear();
        }
        limitLeft -= batch.ResultBatch->num_rows();
        ItemsRead += batch.ResultBatch->num_rows();
    }

    if (limitLeft == 0) {
        DataTasksProcessor.Stop();
        WaitCommitted.clear();
        IndexedData.Abort();
        FetchBlobsQueue.Stop();
    }

    if (WaitCommitted.empty() && !IndexedData.IsInProgress() && FetchBlobsQueue.empty()) {
        DataTasksProcessor.Stop();
        FetchBlobsQueue.Stop();
    }
}

bool TColumnShardScanIterator::HasWaitingTasks() const {
    return DataTasksProcessor.InWaiting();
}

TColumnShardScanIterator::~TColumnShardScanIterator() {
    ReadMetadata->ReadStats->PrintToLog();
}

void TColumnShardScanIterator::Apply(IDataTasksProcessor::ITask::TPtr task) {
    if (!task->IsDataProcessed() || DataTasksProcessor.IsStopped()) {
        return;
    }
    Y_VERIFY(task->Apply(IndexedData));
}

}
