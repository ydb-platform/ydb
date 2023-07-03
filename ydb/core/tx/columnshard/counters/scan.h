#pragma once
#include "common/owner.h"
#include <ydb/core/tx/columnshard/resources/memory.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TScanAggregations {
private:
    using TBase = TCommonCountersOwner;
    std::shared_ptr<NOlap::TMemoryAggregation> ReadBlobs;
    std::shared_ptr<NOlap::TMemoryAggregation> GranulesProcessing;
    std::shared_ptr<NOlap::TMemoryAggregation> GranulesReady;
    std::shared_ptr<NOlap::TMemoryAggregation> ResultsReady;
public:
    TScanAggregations(const TString& moduleId)
        : GranulesProcessing(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Granules/Processing"))
        , ResultsReady(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Results/Ready")) {
    }

    const std::shared_ptr<NOlap::TMemoryAggregation>& GetGranulesProcessing() const {
        return GranulesProcessing;
    }
    const std::shared_ptr<NOlap::TMemoryAggregation>& GetResultsReady() const {
        return ResultsReady;
    }
};

class TScanCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessingOverload;
    NMonitoring::TDynamicCounters::TCounterPtr ReadingOverload;

    NMonitoring::TDynamicCounters::TCounterPtr PriorityFetchBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PriorityFetchCount;
    NMonitoring::TDynamicCounters::TCounterPtr GeneralFetchBytes;
    NMonitoring::TDynamicCounters::TCounterPtr GeneralFetchCount;

    NMonitoring::TDynamicCounters::TCounterPtr NoResultsAckRequest;
public:
    NMonitoring::TDynamicCounters::TCounterPtr PortionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr FilterBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PostFilterBytes;

    NMonitoring::TDynamicCounters::TCounterPtr AssembleFilterCount;

    NMonitoring::TDynamicCounters::TCounterPtr FilterOnlyCount;
    NMonitoring::TDynamicCounters::TCounterPtr FilterOnlyFetchedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr FilterOnlyUsefulBytes;

    NMonitoring::TDynamicCounters::TCounterPtr EmptyFilterCount;
    NMonitoring::TDynamicCounters::TCounterPtr EmptyFilterFetchedBytes;

    NMonitoring::TDynamicCounters::TCounterPtr OriginalRowsCount;
    NMonitoring::TDynamicCounters::TCounterPtr FilteredRowsCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkippedBytes;

    NMonitoring::TDynamicCounters::TCounterPtr TwoPhasesCount;
    NMonitoring::TDynamicCounters::TCounterPtr TwoPhasesFilterFetchedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr TwoPhasesFilterUsefulBytes;
    NMonitoring::TDynamicCounters::TCounterPtr TwoPhasesPostFilterFetchedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr TwoPhasesPostFilterUsefulBytes;

    NMonitoring::TDynamicCounters::TCounterPtr Hanging;

    NMonitoring::THistogramPtr HistogramCacheBlobsCountDuration;
    NMonitoring::THistogramPtr HistogramMissCacheBlobsCountDuration;
    NMonitoring::THistogramPtr HistogramCacheBlobBytesDuration;
    NMonitoring::THistogramPtr HistogramMissCacheBlobBytesDuration;

    TScanCounters(const TString& module = "Scan");

    void OnEmptyAck() const {
        NoResultsAckRequest->Add(1);
    }

    void OnPriorityFetch(const ui64 size) const {
        PriorityFetchBytes->Add(size);
        PriorityFetchCount->Add(1);
    }

    void OnGeneralFetch(const ui64 size) const {
        GeneralFetchBytes->Add(size);
        GeneralFetchCount->Add(1);
    }

    void OnProcessingOverloaded() const {
        ProcessingOverload->Add(1);
    }
    void OnReadingOverloaded() const {
        ReadingOverload->Add(1);
    }

    TScanAggregations BuildAggregations();
};

class TConcreteScanCounters: public TScanCounters {
private:
    using TBase = TScanCounters;
public:
    TScanAggregations Aggregations;

    TConcreteScanCounters(const TScanCounters& counters)
        : TBase(counters)
        , Aggregations(TBase::BuildAggregations())
    {

    }
};

}
