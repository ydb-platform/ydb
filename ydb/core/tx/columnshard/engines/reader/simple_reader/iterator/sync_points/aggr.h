#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TScanWithLimitCollection;

class TSyncPointResultsAggregationControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    std::vector<std::shared_ptr<NCommon::IDataSource>> SourcesToAggregate;
    const std::shared_ptr<ISourcesCollection> Collection;
    const std::shared_ptr<TFetchingScript> AggregationScript;
    ui32 InFlightControl = 0;
    ui32 SourcesCount = 0;

    std::shared_ptr<NCommon::IDataSource> Flush() {
        if (SourcesToAggregate.empty()) {
            return nullptr;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "aggregation_batching")("count", SourcesToAggregate.size());
        ++InFlightControl;
        auto result = std::make_shared<TAggregationDataSource>(std::move(SourcesToAggregate), Context);
        result->SetPurposeSyncPointIndex(0);
        result->SetPurposeSyncPointIndex(GetPointIndex());
        SourcesToAggregate.clear();
        SourcesSequentially.emplace_back(result);
        result->InitFetchingPlan(AggregationScript);
        return result;
    }

    std::shared_ptr<NCommon::IDataSource> TryToFlush() {
        if (SourcesToAggregate.size() >= 100 || (Collection->IsFinished() && Collection->GetSourcesInFlightCount() == SourcesCount) ||
            Collection->GetMaxInFlight() == SourcesCount) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "flush")("to_aggr", SourcesToAggregate.size())(
                "fin", Collection->IsFinished())("fly", Collection->GetSourcesInFlightCount())("count", SourcesCount)(
                "max", Collection->GetMaxInFlight());
            return Flush();
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("to_aggr", SourcesToAggregate.size())("fin", Collection->IsFinished())(
            "fly", Collection->GetSourcesInFlightCount())("count", SourcesCount)("max", Collection->GetMaxInFlight());
        return nullptr;
    }

    virtual bool IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const override {
        return source->IsSyncSection() && source->HasStageResult();
    }

    virtual std::shared_ptr<NCommon::IDataSource> DoOnSourceFinishedOnPreviouse() override {
        return TryToFlush();
    }

    virtual bool IsFinished() const override {
        return ISyncPoint::IsFinished() && SourcesToAggregate.empty();
    }

    virtual std::shared_ptr<NCommon::IDataSource> OnAddSource(const std::shared_ptr<NCommon::IDataSource>& source) override {
        ++SourcesCount;
        SourcesToAggregate.emplace_back(source);
        source->MutableAs<IDataSource>()->ClearMemoryGuards();
        return TryToFlush();
    }

    virtual void DoAbort() override {
        SourcesToAggregate.clear();
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) override {
        AFL_VERIFY(InFlightControl);
        --InFlightControl;
        AFL_VERIFY(!Next);
        AFL_VERIFY(source->GetAs<IDataSource>()->GetType() == IDataSource::EType::Aggregation)(
            "type", source->GetAs<IDataSource>()->GetType());
        const TAggregationDataSource* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
        for (auto&& i : aggrSource->GetSources()) {
            Collection->OnSourceFinished(i);
            AFL_VERIFY(SourcesCount);
            --SourcesCount;
        }
        if (source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        auto resultChunk = source->MutableStageResult().ExtractResultChunk();
        AFL_VERIFY(source->GetStageResult().IsFinished());
        if (resultChunk && resultChunk->HasData()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_id", aggrSource->GetLastSourceId())(
                "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows());
            auto cursor = std::make_shared<TNotSortedSimpleScanCursor>(aggrSource->GetLastSourceId(), aggrSource->GetLastSourceRecordsCount());
            reader.OnIntervalResult(
                std::make_unique<TPartialReadResult>(source->ExtractResourceGuards(), source->MutableAs<IDataSource>()->ExtractGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), std::nullopt));
        }
        source->MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::Finish;
    }

public:
    TSyncPointResultsAggregationControl(const std::shared_ptr<ISourcesCollection>& collection,
        const std::shared_ptr<TFetchingScript>& aggregationScript, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(pointIndex, "SYNC_AGGR", context, nullptr)
        , Collection(collection)
        , AggregationScript(aggregationScript) {
        AFL_VERIFY(AggregationScript);
        AFL_VERIFY(pointIndex);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
