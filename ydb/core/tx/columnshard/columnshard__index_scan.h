#pragma once

#include "columnshard__scan.h"
#include "columnshard_common.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NColumnShard {

class TIndexColumnResolver : public IColumnResolver {
    const NOlap::TIndexInfo& IndexInfo;

public:
    explicit TIndexColumnResolver(const NOlap::TIndexInfo& indexInfo)
        : IndexInfo(indexInfo)
    {}

    TString GetColumnName(ui32 id, bool required) const override {
        return IndexInfo.GetColumnName(id, required);
    }

    const NTable::TScheme::TTableSchema& GetSchema() const override {
        return IndexInfo;
    }
};


using NOlap::TUnifiedBlobId;
using NOlap::TBlobRange;

class TColumnShardScanIterator : public TScanIteratorBase {
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    NOlap::TIndexedReadData IndexedData;
    std::unordered_map<NOlap::TCommittedBlob, ui32, THash<NOlap::TCommittedBlob>> WaitCommitted;
    TVector<TBlobRange> BlobsToRead;
    ui64 NextBlobIdxToRead = 0;
    TDeque<NOlap::TPartialReadResult> ReadyResults;
    bool IsReadFinished = false;
    ui64 ItemsRead = 0;
    const i64 MaxRowsInBatch = 5000;

public:
    TColumnShardScanIterator(NOlap::TReadMetadata::TConstPtr readMetadata)
        : ReadMetadata(readMetadata)
        , IndexedData(ReadMetadata)
    {
        ui32 batchNo = 0;
        for (size_t i = 0; i < ReadMetadata->CommittedBlobs.size(); ++i, ++batchNo) {
            const auto& cmtBlob = ReadMetadata->CommittedBlobs[i];
            WaitCommitted.emplace(cmtBlob, batchNo);
        }
        std::vector<TBlobRange> indexedBlobs = IndexedData.InitRead(batchNo, true);

        // Add cached batches without read
        for (auto& [blobId, batch] : ReadMetadata->CommittedBatches) {
            auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{blobId, 0, 0});
            Y_VERIFY(!cmt.empty());

            const NOlap::TCommittedBlob& cmtBlob = cmt.key();
            ui32 batchNo = cmt.mapped();
            IndexedData.AddNotIndexed(batchNo, batch, cmtBlob.PlanStep, cmtBlob.TxId);
        }

        // Read all committed blobs
        for (const auto& cmtBlob : ReadMetadata->CommittedBlobs) {
            auto& blobId = cmtBlob.BlobId;
            BlobsToRead.push_back(TBlobRange(blobId, 0, blobId.BlobSize()));
        }

        Y_VERIFY(ReadMetadata->IsSorted());

        for (auto&& blobRange : indexedBlobs) {
            BlobsToRead.emplace_back(blobRange);
        }

        IsReadFinished = ReadMetadata->Empty();
    }

    virtual void Apply(IDataPreparationTask::TPtr task) override {
        task->Apply(IndexedData);
    }

    void AddData(const TBlobRange& blobRange, TString data, IDataTasksProcessor::TPtr processor) override {
        const auto& blobId = blobRange.BlobId;
        if (IndexedData.IsIndexedBlob(blobRange)) {
            IndexedData.AddIndexed(blobRange, data, processor);
        } else {
            auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{blobId, 0, 0});
            if (cmt.empty()) {
                return; // ignore duplicates
            }
            const NOlap::TCommittedBlob& cmtBlob = cmt.key();
            ui32 batchNo = cmt.mapped();
            IndexedData.AddNotIndexed(batchNo, data, cmtBlob.PlanStep, cmtBlob.TxId);
        }
    }

    bool Finished() const  override {
        return IsReadFinished && ReadyResults.empty();
    }

    NOlap::TPartialReadResult GetBatch() override {
        FillReadyResults();

        if (ReadyResults.empty()) {
            return {};
        }

        auto result(std::move(ReadyResults.front()));
        ReadyResults.pop_front();

        return result;
    }

    TBlobRange GetNextBlobToRead() override {
        if (IsReadFinished || NextBlobIdxToRead == BlobsToRead.size()) {
            return TBlobRange();
        }
        const auto& blob = BlobsToRead[NextBlobIdxToRead];
        ++NextBlobIdxToRead;
        return blob;
    }

    size_t ReadyResultsCount() const override {
        return ReadyResults.size();
    }

private:
    void FillReadyResults() {
        auto ready = IndexedData.GetReadyResults(MaxRowsInBatch);
        i64 limitLeft = ReadMetadata->Limit == 0 ? INT64_MAX : ReadMetadata->Limit - ItemsRead;
        for (size_t i = 0; i < ready.size() && limitLeft; ++i) {
            if (ready[i].ResultBatch->num_rows() == 0 && !ready[i].LastReadKey) {
                Y_VERIFY(i+1 == ready.size(), "Only last batch can be empty!");
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
            WaitCommitted.clear();
            IndexedData.Abort();
            IsReadFinished = true;
        }

        if (WaitCommitted.empty() && !IndexedData.IsInProgress() && NextBlobIdxToRead == BlobsToRead.size()) {
            IsReadFinished = true;
        }
    }
};

}
