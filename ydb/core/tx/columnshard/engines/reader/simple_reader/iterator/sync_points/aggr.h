#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TScanWithLimitCollection;

class TSyncPointResultsAggregationControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    const ui32 MemoryLimit;
    std::vector<std::shared_ptr<IDataSource>> SourcesToAggregate;
    const std::shared_ptr<TFetchingScript> AggregationScript;
    ui32 SourcesCount = 0;

    std::shared_ptr<IDataSource> Flush() {
        if (SourcesToAggregate.empty()) {
            return nullptr;
        }
        auto result = std::make_shared<TAggregationDataSource>(std::move(SourcesToAggregate), SourcesCount);
        ++SourcesCount;
        SourcesToAggregate.clear();
        SourcesSequentially.emplace_back(result);
        result->InitFetchingPlan(AggregationScript);
        return result;
    }

    virtual bool IsSourcePrepared(const std::shared_ptr<IDataSource>& source) const override {
        return source->IsSyncSection() && source->HasStageData();
    }

    virtual std::shared_ptr<IDataSource> OnSourceFinished() override {
        return Flush();
    }

    virtual bool IsFinished() const override {
        return ISyncPoint::IsFinished() && SourcesToAggregate.empty();
    }

    virtual std::shared_ptr<IDataSource> OnAddSource(const std::shared_ptr<IDataSource>& source) override {
        SourcesToAggregate.emplace_back(source);
        if (SourcesToAggregate.size() > 100) {
            return Flush();
        }
        return nullptr;
    }

    virtual void DoAbort() override {
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) override {
        AFL_VERIFY(!Next);
        if (source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        AFL_VERIFY(source->GetType() == IDataSource::EType::Aggregation)("type", source->GetType());
        const TAggregationDataSource* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
        auto resultChunk = source->MutableStageResult().ExtractResultChunk();
        AFL_VERIFY(source->GetStageResult().IsFinished());
        if (resultChunk && resultChunk->HasData()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_id", source->GetLastSourceId())(
                "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows())("is_finished", isFinished);
            auto cursor = std::make_shared<TNotSortedSimpleScanCursor>(aggrSource->GetLastSourceId(), source->GetLastSourceRecordsCount());
            reader.OnIntervalResult(std::make_unique<TPartialReadResult>(source->ExtractResourceGuards(), source->ExtractGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), std::nullopt));
        }
        source->ClearResult();
        return ESourceAction::Finish;
    }

public:
    TSyncPointResultsAggregationControl(
        const std::shared_ptr<TFetchingScript>& aggregationScript, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(pointIndex, "SYNC_AGGR", context, nullptr)
        , AggregationScript(aggregationScript) {
        AFL_VERIFY(AggregationScript);
        AFL_VERIFY(pointIndex);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
