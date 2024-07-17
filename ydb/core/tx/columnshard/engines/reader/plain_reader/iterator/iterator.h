#pragma once
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TReadyResults {
private:
    const NColumnShard::TConcreteScanCounters Counters;
    std::deque<TPartialReadResult> Data;
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
    TPartialReadResult& emplace_back(TPartialReadResult&& v) {
        RecordsCount += v.GetResultBatch().num_rows();
        Data.emplace_back(std::move(v));
        return Data.back();
    }
    std::optional<TPartialReadResult> pop_front() {
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
    std::shared_ptr<TReadContext> Context;
    const TReadMetadata::TConstPtr ReadMetadata;
    TReadyResults ReadyResults;
    std::shared_ptr<IDataReader> IndexedData;
    ui64 ItemsRead = 0;
    const i64 MaxRowsInBatch = 5000;
    virtual void DoOnSentDataFromInterval(const ui32 intervalIdx) const override;

public:
    TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context, const TReadMetadata::TConstPtr& readMetadata);
    ~TColumnShardScanIterator();

    virtual TConclusionStatus Start() override {
        AFL_VERIFY(IndexedData);
        return IndexedData->Start();
    }

    virtual std::optional<ui32> GetAvailableResultsCount() const override {
        return ReadyResults.size();
    }

    virtual const TReadStats& GetStats() const override {
        return *ReadMetadata->ReadStats;
    }

    virtual TString DebugString(const bool verbose) const override {
        return TStringBuilder()
            << "ready_results:(" << ReadyResults.DebugString() << ");"
            << "indexed_data:(" << IndexedData->DebugString(verbose) << ")"
            ;
    }

    virtual void Apply(const std::shared_ptr<IApplyAction>& task) override;

    bool Finished() const  override {
        return IndexedData->IsFinished() && ReadyResults.empty();
    }

    TConclusion<std::optional<TPartialReadResult>> GetBatch() override;
    virtual void PrepareResults() override;

    virtual TConclusion<bool> ReadNextInterval() override;

private:
    void FillReadyResults();
};

}
