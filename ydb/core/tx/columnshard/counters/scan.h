#pragma once
#include "common/owner.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TScanDataClassAggregation: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeInFlightBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeInFlightCount;
    std::shared_ptr<TValueAggregationClient> InFlightBytes;
    std::shared_ptr<TValueAggregationClient> InFlightCount;
public:
    TScanDataClassAggregation(const TString& moduleId, const TString& signalId)
        : TBase(moduleId)
    {
        DeriviativeInFlightCount = TBase::GetDeriviative(signalId + "/Count");
        DeriviativeInFlightBytes = TBase::GetDeriviative(signalId + "/Bytes");
        InFlightCount = TBase::GetValueAutoAggregationsClient(signalId + "/Count");
        InFlightBytes = TBase::GetValueAutoAggregationsClient(signalId + "/Bytes");
    }

    void AddFullInfo(const ui64 size) {
        DeriviativeInFlightCount->Add(1);
        DeriviativeInFlightBytes->Add(size);
        InFlightCount->Add(1);
        InFlightBytes->Add(size);
    }

    void RemoveFullInfo(const ui64 size) {
        InFlightCount->Remove(1);
        InFlightBytes->Remove(size);
    }

    void AddCount() {
        DeriviativeInFlightCount->Add(1);
        InFlightCount->Add(1);
    }

    void AddBytes(const ui64 size) {
        DeriviativeInFlightBytes->Add(size);
        InFlightBytes->Add(size);
    }
};

class TScanAggregations {
private:
    using TBase = TCommonCountersOwner;
    TScanDataClassAggregation ReadBlobs;
    TScanDataClassAggregation GranulesProcessing;
    TScanDataClassAggregation GranulesReady;
public:
    TScanAggregations(const TString& moduleId)
        : ReadBlobs(moduleId, "InFlight/Blobs/Read")
        , GranulesProcessing(moduleId, "InFlight/Granules/Processing")
        , GranulesReady(moduleId, "InFlight/Granules/Ready")
    {
    }

    void AddFlightReadInfo(const ui64 size) {
        ReadBlobs.AddFullInfo(size);
    }

    void RemoveFlightReadInfo(const ui64 size) {
        ReadBlobs.RemoveFullInfo(size);
    }

    void AddGranuleProcessing() {
        GranulesProcessing.AddCount();
    }

    void AddGranuleProcessingBytes(const ui64 size) {
        GranulesProcessing.AddBytes(size);
    }

    void RemoveGranuleProcessingInfo(const ui64 size) {
        GranulesProcessing.RemoveFullInfo(size);
    }

    void AddGranuleReady(const ui64 size) {
        GranulesReady.AddFullInfo(size);
    }

    void RemoveGranuleReady(const ui64 size) {
        GranulesReady.RemoveFullInfo(size);
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

    std::shared_ptr<TScanAggregations> BuildAggregations();
};

class TConcreteScanCounters: public TScanCounters {
private:
    using TBase = TScanCounters;
public:
    std::shared_ptr<TScanAggregations> Aggregations;

    TConcreteScanCounters(const TScanCounters& counters)
        : TBase(counters)
        , Aggregations(TBase::BuildAggregations())
    {

    }
};

}
