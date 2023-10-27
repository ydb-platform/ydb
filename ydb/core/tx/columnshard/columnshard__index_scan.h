#pragma once

#include "columnshard__scan.h"
#include "columnshard_common.h"
#include "engines/reader/read_metadata.h"
#include "engines/reader/read_context.h"

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
    i64 RecordsCount = 0;
public:
    TString DebugString() const {
        TStringBuilder sb;
        sb
            << "count:" << Data.size() << ";"
            << "records_count:" << RecordsCount << ";"
            ;
        if (Data.size()) {
            sb << "schema=" << Data.front().GetResultBatch().schema()->ToString() << ";";
        }
        return sb;
    }
    TReadyResults(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }
    NOlap::TPartialReadResult& emplace_back(NOlap::TPartialReadResult&& v) {
        RecordsCount += v.GetResultBatch().num_rows();
        Data.emplace_back(std::move(v));
        return Data.back();
    }
    std::optional<NOlap::TPartialReadResult> pop_front() {
        if (Data.empty()) {
            return {};
        }
        auto result = std::move(Data.front());
        RecordsCount -= result.GetResultBatch().num_rows();
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
    std::shared_ptr<NOlap::TReadContext> Context;
    const NOlap::TReadMetadata::TConstPtr ReadMetadata;
    TReadyResults ReadyResults;
    std::shared_ptr<NOlap::IDataReader> IndexedData;
    ui64 ItemsRead = 0;
    const i64 MaxRowsInBatch = 5000;
public:
    TColumnShardScanIterator(const std::shared_ptr<NOlap::TReadContext>& context, const NOlap::TReadMetadata::TConstPtr& readMetadata);
    ~TColumnShardScanIterator();

    virtual std::optional<ui32> GetAvailableResultsCount() const override {
        return ReadyResults.size();
    }

    virtual TString DebugString(const bool verbose) const override {
        return TStringBuilder()
            << "ready_results:(" << ReadyResults.DebugString() << ");"
            << "indexed_data:(" << IndexedData->DebugString(verbose) << ")"
            ;
    }

    virtual void Apply(IDataTasksProcessor::ITask::TPtr task) override;

    bool Finished() const  override {
        return IndexedData->IsFinished() && ReadyResults.empty();
    }

    std::optional<NOlap::TPartialReadResult> GetBatch() override;
    virtual void PrepareResults() override;

    virtual bool ReadNextInterval() override;

private:
    void FillReadyResults();
};

}
