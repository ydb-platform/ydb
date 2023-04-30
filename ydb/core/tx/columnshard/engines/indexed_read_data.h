#pragma once
#include "defs.h"
#include "column_engine.h"
#include "predicate.h"
#include "reader/queue.h"
#include "reader/granule.h"
#include "reader/batch.h"
#include "reader/filling_context.h"
#include "reader/read_metadata.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/counters.h>

namespace NKikimr::NColumnShard {
class TScanIteratorBase;
}

namespace NKikimr::NOlap {

class TIndexedReadData {
private:
    std::unique_ptr<NIndexedReader::TGranulesFillingContext> GranulesContext;

    YDB_READONLY_DEF(NColumnShard::TScanCounters, Counters);
    YDB_READONLY_DEF(NColumnShard::TDataTasksProcessorContainer, TasksProcessor);
    TFetchBlobsQueue& FetchBlobsQueue;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    bool OnePhaseReadMode = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

    THashSet<const void*> BatchesToDedup;
    THashMap<TBlobRange, NIndexedReader::TBatch*> IndexedBlobSubscriber; // blobId -> batch
    THashSet<TBlobRange> IndexedBlobs;
    ui32 ReadyNotIndexed{ 0 };
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> OutNotIndexed; // granule -> not indexed to append
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata, TFetchBlobsQueue& fetchBlobsQueue,
        const bool internalRead, const NColumnShard::TScanCounters& counters, NColumnShard::TDataTasksProcessorContainer tasksProcessor);

    NIndexedReader::TGranulesFillingContext& GetGranulesContext() {
        Y_VERIFY(GranulesContext);
        return *GranulesContext;
    }

    /// Initial FetchBlobsQueue filling (queue from external scan iterator). Granules could be read independently
    void InitRead(ui32 numNotIndexed);
    void Abort() {
        Y_VERIFY(GranulesContext);
            return GranulesContext->Abort();
    }
    bool IsInProgress() const {
        Y_VERIFY(GranulesContext);
        return GranulesContext->IsInProgress();
    }

    /// @returns batches and corresponding last keys in correct order (i.e. sorted by by PK)
    TVector<TPartialReadResult> GetReadyResults(const int64_t maxRowsInBatch);

    void AddNotIndexed(ui32 batchNo, TString blob, ui64 planStep, ui64 txId) {
        auto batch = NArrow::DeserializeBatch(blob, ReadMetadata->BlobSchema);
        AddNotIndexed(batchNo, batch, planStep, txId);
    }

    void AddNotIndexed(ui32 batchNo, const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) {
        Y_VERIFY(batchNo < NotIndexed.size());
        if (!NotIndexed[batchNo]) {
            ++ReadyNotIndexed;
        }
        NotIndexed[batchNo] = MakeNotIndexedBatch(batch, planStep, txId);
    }

    void AddIndexed(const TBlobRange& blobRange, const TString& column);
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobs.contains(blobRange);
    }
    NOlap::TReadMetadata::TConstPtr GetReadMetadata() const {
        return ReadMetadata;
    }

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
        if (batch && batch->num_rows()) {
            ReadMetadata->ReadStats->SelectedRows += batch->num_rows();
            if (batchInfo.IsDuplicationsAvailable()) {
                Y_VERIFY(batchInfo.GetOwner().IsDuplicationsAvailable());
                BatchesToDedup.insert(batch.get());
            } else {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, IndexInfo().GetReplaceKey(), false));
            }
        }
    }

private:
    const TIndexInfo& IndexInfo() const {
        return ReadMetadata->IndexInfo;
    }

    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, ui64 planStep, ui64 txId) const;

    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyToOut();
    TVector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
