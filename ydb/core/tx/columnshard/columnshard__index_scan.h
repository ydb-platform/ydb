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

class TColumnShardScanIterator: public TScanIteratorBase {
private:
    NOlap::TReadContext Context;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    NOlap::TIndexedReadData IndexedData;
    std::unordered_map<NOlap::TCommittedBlob, ui32, THash<NOlap::TCommittedBlob>> WaitCommitted;
    TDeque<NOlap::TPartialReadResult> ReadyResults;
    ui64 ItemsRead = 0;
    const i64 MaxRowsInBatch = 5000;
public:
    TColumnShardScanIterator(NOlap::TReadMetadata::TConstPtr readMetadata, const NOlap::TReadContext& context);
    ~TColumnShardScanIterator();

    virtual std::optional<ui32> GetAvailableResultsCount() const override {
        return ReadyResults.size();
    }

    virtual TString DebugString() const override {
        return TStringBuilder()
            << "indexed_data:(" << IndexedData.DebugString() << ")"
            ;
    }

    virtual void Apply(IDataTasksProcessor::ITask::TPtr task) override;

    virtual bool HasWaitingTasks() const override;

    void AddData(const TBlobRange& blobRange, TString data) override;

    bool Finished() const  override {
        return IndexedData.IsFinished() && ReadyResults.empty();
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
