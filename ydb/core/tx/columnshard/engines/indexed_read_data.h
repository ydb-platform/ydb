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
    TReadContext Context;
    std::unique_ptr<NIndexedReader::TGranulesFillingContext> GranulesContext;
    THashMap<TUnifiedBlobId, NOlap::TCommittedBlob> WaitCommitted;

    TFetchBlobsQueue FetchBlobsQueue;
    TFetchBlobsQueue PriorityBlobsQueue;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    bool OnePhaseReadMode = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

    THashMap<TBlobRange, NIndexedReader::TBatch*> IndexedBlobSubscriber;
    std::shared_ptr<arrow::RecordBatch> NotIndexedOutscopeBatch;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

    void AddNotIndexed(const TBlobRange& blobRange, const TString& column);
    void AddNotIndexed(const TUnifiedBlobId& blobId, const std::shared_ptr<arrow::RecordBatch>& batch);
    void RegisterZeroGranula();

    void AddIndexed(const TBlobRange& blobRange, const TString& column);
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobSubscriber.contains(blobRange);
    }
public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata, const bool internalRead, const TReadContext& context);

    TString DebugString() const {
        return TStringBuilder()
            << "internal:" << OnePhaseReadMode << ";"
            << "wait_committed:" << WaitCommitted.size() << ";"
            << "granules_context:(" << (GranulesContext ? GranulesContext->DebugString() : "NO") << ");"
            ;
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const noexcept {
        return Context.GetCounters();
    }

    const NColumnShard::TDataTasksProcessorContainer& GetTasksProcessor() const noexcept {
        return Context.GetProcessor();
    }

    NIndexedReader::TGranulesFillingContext& GetGranulesContext() {
        Y_VERIFY(GranulesContext);
        return *GranulesContext;
    }

    void InitRead();
    void Abort();
    bool IsFinished() const;

    /// @returns batches and corresponding last keys in correct order (i.e. sorted by by PK)
    std::vector<TPartialReadResult> GetReadyResults(const int64_t maxRowsInBatch);

    void AddData(const TBlobRange& blobRange, const TString& data) {
        if (IsIndexedBlob(blobRange)) {
            AddIndexed(blobRange, data);
        } else {
            AddNotIndexed(blobRange, data);
        }
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
