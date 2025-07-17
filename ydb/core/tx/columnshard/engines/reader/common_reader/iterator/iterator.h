#pragma once
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TReadMetadata;

class TReadyResults {
private:
    const NColumnShard::TConcreteScanCounters Counters;
    std::deque<std::shared_ptr<TPartialReadResult>> Data;
    i64 RecordsCount = 0;
public:
    TString DebugString() const {
        TStringBuilder sb;
        sb
            << "count:" << Data.size() << ";"
            << "records_count:" << RecordsCount << ";"
            ;
        if (Data.size()) {
            sb << "schema=" << Data.front()->GetResultBatch().schema()->ToString() << ";";
        }
        return sb;
    }
    TReadyResults(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }
    const std::shared_ptr<TPartialReadResult>& emplace_back(std::shared_ptr<TPartialReadResult>&& v) {
        AFL_VERIFY(!!v);
        RecordsCount += v->GetResultBatch().num_rows();
        Data.emplace_back(std::move(v));
        return Data.back();
    }
    std::shared_ptr<TPartialReadResult> pop_front() {
        if (Data.empty()) {
            return {};
        }
        auto result = std::move(Data.front());
        RecordsCount -= result->GetResultBatch().num_rows();
        AFL_VERIFY(RecordsCount >= 0);
        Data.pop_front();
        return std::move(result);
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
    virtual void DoOnSentDataFromInterval(const TPartialSourceAddress& address) override;

protected:
    ui64 ItemsRead = 0;
    const i64 MaxRowsInBatch = 5000;
    std::shared_ptr<TReadContext> Context;
    std::shared_ptr<const TReadMetadata> ReadMetadata;
    TReadyResults ReadyResults;
    std::shared_ptr<IDataReader> IndexedData;

public:
    TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context);
    ~TColumnShardScanIterator();

    virtual TConclusionStatus Start() override {
        AFL_VERIFY(IndexedData);
        return IndexedData->Start();
    }

    virtual std::optional<ui32> GetAvailableResultsCount() const override {
        return ReadyResults.size();
    }

    virtual const TReadStats& GetStats() const override;

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

    virtual TConclusion<std::shared_ptr<TPartialReadResult>> GetBatch() override;
    virtual void PrepareResults() override;

    virtual TConclusion<bool> ReadNextInterval() override;

private:
    virtual void FillReadyResults() = 0;
};

}
