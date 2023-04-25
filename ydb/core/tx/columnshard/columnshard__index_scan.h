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
    TColumnShardScanIterator(NOlap::TReadMetadata::TConstPtr readMetadata);

    virtual void Apply(IDataPreparationTask::TPtr task) override {
        task->Apply(IndexedData);
    }

    void AddData(const TBlobRange& blobRange, TString data, IDataTasksProcessor::TPtr processor) override;

    bool Finished() const  override {
        return IsReadFinished && ReadyResults.empty();
    }

    NOlap::TPartialReadResult GetBatch() override;

    TBlobRange GetNextBlobToRead() override;

    size_t ReadyResultsCount() const override {
        return ReadyResults.size();
    }

private:
    void FillReadyResults();
};

}
