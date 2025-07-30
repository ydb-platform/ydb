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
    const std::shared_ptr<TFetchingScript> RestoreResultScript;
    ui32 InFlightControl = 0;
    ui32 SourcesCount = 0;
    bool AggregationActivity = true;
    ui32 AggregationsCount = 0;
    ui32 UselessAggregationsCount = 0;

    static inline const double CriticalBadAggregationKffForSource = 1.5;
    static const ui32 GuaranteeNeedAggregationSourceRecordsCount = 1000;

    static const ui32 AggregationPackSize = 100;

    static const ui32 AggregatedResultKeysCountMinimalForControl = 10000;
    static inline const double CriticalBadAggregationKffForAggregation = 5;

    static inline const double UselessDetectorFractionKff = 0.5;
    static const ui32 UselessDetectorCountLimit = 7;

    virtual TString DoDebugString() const override {
        TStringBuilder sb;
        sb << "{";
        sb << SourcesToAggregate.size() << ",";
        sb << InFlightControl << ",";
        sb << SourcesCount << ",";
        sb << AggregationActivity << ",";
        sb << AggregationsCount << ",";
        sb << UselessAggregationsCount;
        sb << "}";
        return sb;
    }

    std::shared_ptr<NCommon::IDataSource> Flush() {
        if (SourcesToAggregate.empty()) {
            return nullptr;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "aggregation_batching")("count", SourcesToAggregate.size());
        ++InFlightControl;
        auto result = std::make_shared<TAggregationDataSource>(std::move(SourcesToAggregate), Context);
        result->InitPurposeSyncPointIndex(GetPointIndex());
        SourcesToAggregate.clear();
        SourcesSequentially.emplace_back(result);
        result->InitFetchingPlan(AggregationScript);
        return result;
    }

    std::shared_ptr<NCommon::IDataSource> TryToFlush() {
        if (!AggregationActivity || SourcesToAggregate.size() >= AggregationPackSize ||
            (Collection->IsFinished() && Collection->GetSourcesInFlightCount() == SourcesCount) ||
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
        bool localAggregationActivity = true;
        if (SourcesToAggregate.empty()) {
            if (AggregationActivity) {
                ui32 originalCount = source->GetRecordsCount();
                if (!source->GetStageData().GetTable()->GetFilter().IsTotalAllowFilter()) {
                    originalCount = source->GetStageData().GetTable()->GetFilter().GetFilteredCountVerified();
                }
                const ui32 aggrKeysCount = source->GetStageData().GetTable()->GetRecordsCountActualVerified();
                localAggregationActivity =
                    aggrKeysCount < GuaranteeNeedAggregationSourceRecordsCount || aggrKeysCount * CriticalBadAggregationKffForSource < originalCount;
            } else {
                localAggregationActivity = false;
            }
        }
        ++SourcesCount;
        if (localAggregationActivity) {
            SourcesToAggregate.emplace_back(source);
            source->MutableAs<IDataSource>()->ClearMemoryGuards();
            return TryToFlush();
        } else {
            ++InFlightControl;
            SourcesSequentially.emplace_back(source);
            source->MutableAs<IDataSource>()->InitFetchingPlan(RestoreResultScript);
            return source;
        }
    }

    virtual void DoAbort() override {
        SourcesToAggregate.clear();
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) override {
        AFL_VERIFY(InFlightControl);
        --InFlightControl;
        AFL_VERIFY(!Next);
        std::shared_ptr<IScanCursor> cursor;
        if (source->GetAs<IDataSource>()->GetType() == IDataSource::EType::Aggregation) {
            const TAggregationDataSource* aggrSource = static_cast<const TAggregationDataSource*>(source.get());
            for (auto&& i : aggrSource->GetSources()) {
                Collection->OnSourceFinished(i);
                AFL_VERIFY(SourcesCount);
                --SourcesCount;
            }
            cursor = std::make_shared<TNotSortedSimpleScanCursor>(aggrSource->GetLastSourceId(), aggrSource->GetLastSourceRecordsCount());
        } else {
            Collection->OnSourceFinished(source);
            cursor = std::make_shared<TNotSortedSimpleScanCursor>(source->GetSourceId(), source->GetRecordsCount());
            AFL_VERIFY(SourcesCount);
            --SourcesCount;
        }
        AFL_VERIFY(!source->GetStageResult().IsEmpty());
        auto resultChunk = source->MutableStageResult().ExtractResultChunk();
        AFL_VERIFY(source->GetStageResult().IsFinished());
        AFL_VERIFY(resultChunk && resultChunk->HasData());
        if (AggregationActivity) {
            ++AggregationsCount;
            if (resultChunk->GetTable()->num_rows() > AggregatedResultKeysCountMinimalForControl &&
                source->GetRecordsCount() < CriticalBadAggregationKffForAggregation * resultChunk->GetTable()->num_rows()) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "useless_aggregation")("source_id", source->GetSourceId())("source_idx",
                    source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows())("original_count", source->GetRecordsCount())(
                    "activity", AggregationActivity)("useless_count", UselessAggregationsCount)("aggr_count", AggregationsCount);
                if (++UselessAggregationsCount > UselessDetectorFractionKff * AggregationsCount &&
                    AggregationsCount > UselessDetectorCountLimit) {
                    AggregationActivity = false;
                }
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_id", source->GetSourceId())(
            "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows())("original_count", source->GetRecordsCount())(
            "activity", AggregationActivity);
        reader.OnIntervalResult(
            std::make_unique<TPartialReadResult>(source->ExtractResourceGuards(), source->MutableAs<IDataSource>()->ExtractGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), std::nullopt));
        source->MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::Finish;
    }

public:
    TSyncPointResultsAggregationControl(const std::shared_ptr<ISourcesCollection>& collection,
        const std::shared_ptr<TFetchingScript>& aggregationScript, const std::shared_ptr<TFetchingScript>& restoreResultScript,
        const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(pointIndex, "SYNC_AGGR", context, nullptr)
        , Collection(collection)
        , AggregationScript(aggregationScript)
        , RestoreResultScript(restoreResultScript) {
        AFL_VERIFY(AggregationScript);
        AFL_VERIFY(RestoreResultScript);
        AFL_VERIFY(pointIndex);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
