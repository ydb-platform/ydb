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

class TIndexedReadData: public IDataReader, TNonCopyable {
private:
    using TBase = IDataReader;
    std::unique_ptr<NIndexedReader::TGranulesFillingContext> GranulesContext;
    THashMap<TUnifiedBlobId, NOlap::TCommittedBlob> WaitCommitted;

    TFetchBlobsQueue FetchBlobsQueue;
    TFetchBlobsQueue PriorityBlobsQueue;
    bool OnePhaseReadMode = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> NotIndexed;

    THashMap<TBlobRange, NIndexedReader::TBatchAddress> IndexedBlobSubscriber;
    std::shared_ptr<arrow::RecordBatch> NotIndexedOutscopeBatch;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription;

    void AddNotIndexed(const TBlobRange& blobRange, const TString& column);
    void AddNotIndexed(const TUnifiedBlobId& blobId, const std::shared_ptr<arrow::RecordBatch>& batch);
    void RegisterZeroGranula();

    void AddIndexed(const TBlobRange& blobRange, const TString& column);
    bool IsIndexedBlob(const TBlobRange& blobRange) const {
        return IndexedBlobSubscriber.contains(blobRange);
    }
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder()
            << "wait_committed:" << WaitCommitted.size() << ";"
            << "granules_context:(" << (GranulesContext ? GranulesContext->DebugString() : "NO") << ");"
            ;
    }

    /// @returns batches and corresponding last keys in correct order (i.e. sorted by by PK)
    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t maxRowsInBatch) override;

    virtual void DoAbort() override;
    virtual bool DoIsFinished() const override;

    virtual void DoAddData(const TBlobRange& blobRange, const TString& data) override;
    virtual std::optional<TBlobRange> DoExtractNextBlob(const bool hasReadyResults) override;
public:
    TIndexedReadData(NOlap::TReadMetadata::TConstPtr readMetadata, const TReadContext& context);

    NIndexedReader::TGranulesFillingContext& GetGranulesContext() {
        Y_VERIFY(GranulesContext);
        return *GranulesContext;
    }

    void InitRead();

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

private:
    std::shared_ptr<arrow::RecordBatch> MakeNotIndexedBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot) const;

    std::shared_ptr<arrow::RecordBatch> MergeNotIndexed(
        std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches) const;
    std::vector<TPartialReadResult> ReadyToOut(const i64 maxRowsInBatch);
    void MergeTooSmallBatches(const std::shared_ptr<TMemoryAggregation>& memAggregation, std::vector<TPartialReadResult>& out) const;
    std::vector<TPartialReadResult> MakeResult(
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&& granules, int64_t maxRowsInBatch) const;
};

}
