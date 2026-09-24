#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>

namespace NKikimr::NOlap::NReader::NSimple {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

class TOrderedResultWithLimitCollection;

class TSyncPointResultsAggregationControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    std::vector<std::unique_ptr<NCommon::TDataSourceLease>> SourcesToAggregate;
    const std::shared_ptr<ISourcesCollection> Collection;
    const std::shared_ptr<TFetchingScript> AggregationScript;
    const std::shared_ptr<TFetchingScript> RestoreResultScript;
    TPositiveControlInteger InFlightControl;
    TPositiveControlInteger SourcesCount;
    bool AggregationActivity = true;
    ui32 AggregationsCount = 0;
    ui32 UselessAggregationsCount = 0;

    static inline const double CriticalBadAggregationKffForSource = 1.5;
    static const ui32 GuaranteeNeedAggregationSourceRecordsCount = 1000;
    static const ui64 AggregationMemorySize = ((ui64)8) << 20;

    static const ui32 AggregationPackSize = 10000;

    static const ui32 AggregatedResultKeysCountMinimalForControl = 10000;
    static inline const double CriticalBadAggregationKffForAggregation = 5;

    static inline const double UselessDetectorFractionKff = 0.5;
    static const ui32 UselessDetectorCountLimit = 7;

    TPositiveControlInteger MemoryToAggregate;

    virtual TString DoDebugString() const override {
        TStringBuilder sb;
        sb << "{";
        sb << SourcesToAggregate.size() << ",";
        sb << MemoryToAggregate << ",";
        sb << InFlightControl << ",";
        sb << SourcesCount << ",";
        sb << AggregationActivity << ",";
        sb << AggregationsCount << ",";
        sb << UselessAggregationsCount;
        sb << "}";
        return sb;
    }

    std::unique_ptr<NCommon::TDataSourceLease> Flush() {
        if (SourcesToAggregate.empty()) {
            return nullptr;
        }
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "aggregation_batching"},
            {"count", SourcesToAggregate.size()});
        ++InFlightControl;
        auto result = std::make_shared<TAggregationDataSource>(std::move(SourcesToAggregate), Context);
        result->InitPurposeSyncPointIndex(GetPointIndex());
        SourcesToAggregate.clear();
        MemoryToAggregate = 0;
        SourcesSequentially.emplace_back(*result);
        result->InitFetchingPlan(AggregationScript);
        return std::make_unique<NCommon::TDataSourceLease>(std::move(result));
    }

    std::unique_ptr<NCommon::TDataSourceLease> TryToFlush() {
        if (!AggregationActivity || SourcesToAggregate.size() >= AggregationPackSize || MemoryToAggregate.Val() >= AggregationMemorySize ||
            (Collection->IsFinished() && Collection->GetSourcesInFlightCount() == SourcesCount.Val()) ||
            Collection->GetMaxInFlight() == SourcesCount.Val()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "flush"},
                {"toAggr", SourcesToAggregate.size()},
                {"fin", Collection->IsFinished()},
                {"fly", Collection->GetSourcesInFlightCount()},
                {"count", SourcesCount},
                {"max", Collection->GetMaxInFlight()},
                {"memory", MemoryToAggregate.Val()});
            return Flush();
        }
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"toAggr", SourcesToAggregate.size()},
            {"fin", Collection->IsFinished()},
            {"fly", Collection->GetSourcesInFlightCount()},
            {"count", SourcesCount},
            {"max", Collection->GetMaxInFlight()});
        return nullptr;
    }

    virtual bool IsSourcePrepared(const NCommon::IDataSource& source) const override {
        return source.IsSyncSection() && source.HasStageResult();
    }

    virtual std::unique_ptr<NCommon::TDataSourceLease> DoOnSourceFinishedOnPreviouse() override {
        return TryToFlush();
    }

    virtual bool IsFinished() const override {
        return ISyncPoint::IsFinished() && SourcesToAggregate.empty();
    }

    virtual std::unique_ptr<NCommon::TDataSourceLease> OnAddSource(std::unique_ptr<NCommon::TDataSourceLease> lease) override {
        auto& source = lease->GetSource();
        bool localAggregationActivity = true;
        if (SourcesToAggregate.empty()) {
            if (AggregationActivity) {
                ui32 originalCount = source.GetRecordsCount();
                if (!source.GetStageData().GetTable().GetFilter().IsTotalAllowFilter()) {
                    originalCount = source.GetStageData().GetTable().GetFilter().GetFilteredCountVerified();
                }
                const ui32 aggrKeysCount = source.GetStageData().GetTable().GetRecordsCountActualVerified();
                localAggregationActivity = aggrKeysCount < GuaranteeNeedAggregationSourceRecordsCount ||
                                           aggrKeysCount * CriticalBadAggregationKffForSource < originalCount;
            } else {
                localAggregationActivity = false;
            }
        }
        ++SourcesCount;
        if (localAggregationActivity) {
            MemoryToAggregate += source.GetReservedMemory();
            if (InFlightControl.Val() == 0) {
                source.MutableAs<IDataSource>()->ClearMemoryGuards();
            }
            SourcesToAggregate.emplace_back(std::move(lease));
            return TryToFlush();
        } else {
            ++InFlightControl;
            SourcesSequentially.emplace_back(source);
            source.MutableAs<IDataSource>()->InitFetchingPlan(RestoreResultScript);
            return lease;
        }
    }

    virtual void DoAbort() override {
        MemoryToAggregate = 0;
        SourcesToAggregate.clear();
    }

    virtual ESourceAction OnSourceReady(const NCommon::TDataSourceLease& lease, TPlainReadData& reader) override {
        auto& source = lease.GetSource();
        LWTRACK(SyncAggrSyncPoint, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(),
            source.GetSourceId(), GetPointName(), source.GetFilteredRowsCount(), source.GetReservedMemory(),
            source.GetSourcesAheadQueueWaitDuration(), source.GetSourcesAhead(), DebugString());
        --InFlightControl;
        if (InFlightControl.Val() == 0) {
            for (auto&& i : SourcesToAggregate) {
                i->GetSource().MutableAs<IDataSource>()->ClearMemoryGuards();
            }
        }
        AFL_VERIFY(!Next);
        const auto sourcesSorting = SourcesSortingToProto(Context->GetReadMetadata()->GetSourcesSorting());
        std::shared_ptr<IScanCursor> cursor;
        if (source.GetType() == IDataSource::EType::SimpleAggregation) {
            const auto& aggrSource = static_cast<const TAggregationDataSource&>(source);
            for (auto&& i : aggrSource.GetSources()) {
                Collection->OnSourceFinished(i->GetSource());
                --SourcesCount;
            }
            cursor = std::make_shared<TSourceIndexScanCursor>(sourcesSorting, nullptr, aggrSource.GetLastSourceIdx(),
                aggrSource.GetLastSourceRecordsCount(), aggrSource.GetLastPortionIdOptional());
        } else {
            AFL_VERIFY(source.GetType() == IDataSource::EType::SimplePortion);
            Collection->OnSourceFinished(source);
            cursor = std::make_shared<TSourceIndexScanCursor>(
                sourcesSorting, nullptr, source.GetSourceIdx(), source.GetRecordsCount(), source.GetPortionIdOptional());
            --SourcesCount;
        }
        AFL_VERIFY(!source.GetStageResult().IsEmpty());
        auto resultChunk = source.MutableStageResult().ExtractResultChunk();
        AFL_VERIFY(source.GetStageResult().IsFinished());
        AFL_VERIFY(resultChunk && resultChunk->HasData());
        if (AggregationActivity) {
            ++AggregationsCount;
            if (resultChunk->GetTable()->num_rows() > AggregatedResultKeysCountMinimalForControl &&
                source.GetRecordsCount() < CriticalBadAggregationKffForAggregation * resultChunk->GetTable()->num_rows()) {
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "useless_aggregation"},
                    {"sourceIdx", source.GetSourceIdx()},
                    {"table", resultChunk->GetTable()->num_rows()},
                    {"originalCount", source.GetRecordsCount()},
                    {"activity", AggregationActivity},
                    {"uselessCount", UselessAggregationsCount},
                    {"aggrCount", AggregationsCount});
                if (++UselessAggregationsCount > UselessDetectorFractionKff * AggregationsCount &&
                    AggregationsCount > UselessDetectorCountLimit) {
                    AggregationActivity = false;
                }
            }
        }
        YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "has_result"},
            {"sourceIdx", source.GetSourceIdx()},
            {"table", resultChunk->GetTable()->num_rows()},
            {"originalCount", source.GetRecordsCount()},
            {"activity", AggregationActivity});
        reader.OnIntervalResult(
            std::make_unique<TPartialReadResult>(source.ExtractResourceGuards(), source.MutableAs<IDataSource>()->ExtractGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), std::nullopt, source.GetSourceId()));
        source.MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::Finish;
    }

public:
    TSyncPointResultsAggregationControl(const std::shared_ptr<ISourcesCollection>& collection,
        const std::shared_ptr<TFetchingScript>& aggregationScript, const std::shared_ptr<TFetchingScript>& restoreResultScript,
        const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(pointIndex, "SYNC_AGGR", context, nullptr)
        , Collection(collection)
        , AggregationScript(aggregationScript)
        , RestoreResultScript(restoreResultScript)
    {
        AFL_VERIFY(AggregationScript);
        AFL_VERIFY(RestoreResultScript);
        AFL_VERIFY(pointIndex);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
