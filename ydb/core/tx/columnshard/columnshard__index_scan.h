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

class TReadyResults {
private:
    const NColumnShard::TConcreteScanCounters Counters;
    std::deque<NOlap::TPartialReadResult> Data;
    i64 SumSize = 0;
    i64 RecordsCount = 0;
public:
    TString DebugString() const {
        TStringBuilder sb;
        sb
            << "count:" << Data.size() << ";"
            << "records_count:" << RecordsCount << ";"
            << "sum_size:" << SumSize << ";"
            ;
        if (Data.size()) {
            sb << "schema=" << Data.front().GetResultBatch()->schema()->ToString() << ";";
        }
        return sb;
    }
    TReadyResults(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }
    NOlap::TPartialReadResult& emplace_back(NOlap::TPartialReadResult&& v) {
        SumSize += v.GetSize();
        RecordsCount += v.GetResultBatch()->num_rows();
        Data.emplace_back(std::move(v));
        return Data.back();
    }
    NOlap::TPartialReadResult pop_front() {
        if (Data.empty()) {
            return NOlap::TPartialReadResult();
        }
        auto result = std::move(Data.front());
        SumSize -= result.GetSize();
        RecordsCount -= result.GetResultBatch()->num_rows();
        Data.pop_front();
        return result;
    }
    bool empty() const {
        return Data.empty();
    }
    size_t size() const {
        return Data.size();
    }
};

class TColumnShardScanIterator: public TScanIteratorBase {
private:
    NOlap::TReadContext Context;
    TReadyResults ReadyResults;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    NOlap::TIndexedReadData IndexedData;
    std::unordered_map<NOlap::TCommittedBlob, ui32, THash<NOlap::TCommittedBlob>> WaitCommitted;
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

private:
    void FillReadyResults();
};

}
