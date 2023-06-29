#pragma once
#include "defs.h"
#include "column_engine.h"
#include "predicate/predicate.h"
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

    NColumnShard::TConcreteScanCounters Counters;
    NColumnShard::TDataTasksProcessorContainer TasksProcessor;
    TFetchBlobsQueue FetchBlobsQueue;
    TFetchBlobsQueue PriorityBlobsQueue;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    bool OnePhaseReadMode = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

    THashMap<TBlobRange, NIndexedReader::TBatch*> IndexedBlobSubscriber; // blobId -> batch
    THashSet<TBlobRange> IndexedBlobs;
    ui32 ReadyNotIndexed{ 0 };
    std::shared_ptr<arrow::RecordBatch> NotIndexedOutscopeBatch; // outscope granules batch
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata,
        const bool internalRead, const NColumnShard::TConcreteScanCounters& counters, NColumnShard::TDataTasksProcessorContainer tasksProcessor);

    const NColumnShard::TConcreteScanCounters& GetCounters() const noexcept {
        return Counters;
    }

    const NColumnShard::TDataTasksProcessorContainer& GetTasksProcessor() const noexcept {
        return TasksProcessor;
    }

    NIndexedReader::TGranulesFillingContext& GetGranulesContext() {
        Y_VERIFY(GranulesContext);
        return *GranulesContext;
    }

    /// Initial FetchBlobsQueue filling (queue from external scan iterator). Granules could be read independently
    void InitRead(ui32 numNotIndexed);
    void Abort();
    bool IsFinished() const;

    /// @returns batches and corresponding last keys in correct order (i.e. sorted by by PK)
    std::vector<TPartialReadResult> GetReadyResults(const int64_t maxRowsInBatch);

    void AddNotIndexed(ui32 batchNo, TString blob, const TCommittedBlob& commitedBlob) {
        auto batch = NArrow::DeserializeBatch(blob, ReadMetadata->GetBlobSchema(commitedBlob.GetSchemaSnapshot()));
        AddNotIndexed(batchNo, batch, commitedBlob);
    }

    void AddNotIndexed(ui32 batchNo, const std::shared_ptr<arrow::RecordBatch>& batch, const TCommittedBlob& commitedBlob) {
        Y_VERIFY(batchNo < NotIndexed.size());
        Y_VERIFY(!NotIndexed[batchNo]);
        ++ReadyNotIndexed;
        NotIndexed[batchNo] = MakeNotIndexedBatch(batch, commitedBlob.GetSchemaSnapshot());
    }

    void AddIndexed(const TBlobRange& blobRange, const TString& column);
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobs.contains(blobRange);
    }
    NOlap::TReadMetadata::TConstPtr GetReadMetadata() const {
        return ReadMetadata;
    }

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnBatchReady(const NIndexedReader::TBatch& /*batchInfo*/, std::shared_ptr<arrow::RecordBatch> batch) {
        if (batch && batch->num_rows()) {
            ReadMetadata->ReadStats->SelectedRows += batch->num_rows();
        }
    }

    void AddBlobToFetchInFront(const ui64 granuleId, const TBlobRange& range) {
        PriorityBlobsQueue.emplace_back(granuleId, range);
    }

    bool HasMoreBlobs() const {
        return FetchBlobsQueue.size() || PriorityBlobsQueue.size();
    }

    TBlobRange ExtractNextBlob();

private:
    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot) const;

    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> ReadyToOut();
    std::vector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
