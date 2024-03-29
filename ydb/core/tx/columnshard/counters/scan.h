#pragma once
#include "common/owner.h"
#include <ydb/core/tx/columnshard/resources/memory.h>
#include <ydb/core/tx/columnshard/resource_subscriber/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TScanAggregations: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::shared_ptr<NOlap::TMemoryAggregation> ReadBlobs;
    std::shared_ptr<NOlap::TMemoryAggregation> GranulesProcessing;
    std::shared_ptr<NOlap::TMemoryAggregation> GranulesReady;
    std::shared_ptr<NOlap::TMemoryAggregation> ResultsReady;
    std::shared_ptr<TValueAggregationClient> ScanDuration;
    std::shared_ptr<TValueAggregationClient> BlobsWaitingDuration;
public:
    TScanAggregations(const TString& moduleId)
        : TBase(moduleId)
        , GranulesProcessing(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Granules/Processing"))
        , ResultsReady(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Results/Ready"))
        , ScanDuration(TBase::GetValueAutoAggregationsClient("ScanDuration"))
        , BlobsWaitingDuration(TBase::GetValueAutoAggregationsClient("BlobsWaitingDuration"))
    {

    }

    void OnBlobWaitingDuration(const TDuration d, const TDuration fullScanDuration) const {
        BlobsWaitingDuration->Add(d.MicroSeconds());
        ScanDuration->SetValue(fullScanDuration.MicroSeconds());
    }

    const std::shared_ptr<NOlap::TMemoryAggregation>& GetGranulesProcessing() const {
        return GranulesProcessing;
    }
    const std::shared_ptr<NOlap::TMemoryAggregation>& GetResultsReady() const {
        return ResultsReady;
    }
};

class TScanCounters: public TCommonCountersOwner {
public:
    enum class EStatusFinish {
        Success /* "Success" */ = 0,
        ConveyorInternalError /* "ConveyorInternalError" */,
        ExternalAbort /* "ExternalAbort" */,
        IteratorInternalErrorScan /* "IteratorInternalErrorScan" */,
        IteratorInternalErrorResult /* "IteratorInternalErrorResult" */,
        Deadline /* "Deadline" */,
        UndeliveredEvent /* "UndeliveredEvent" */,
        CannotAddInFlight /* "CannotAddInFlight" */,

        COUNT
    };
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessingOverload;
    NMonitoring::TDynamicCounters::TCounterPtr ReadingOverload;

    NMonitoring::TDynamicCounters::TCounterPtr PriorityFetchBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PriorityFetchCount;
    NMonitoring::TDynamicCounters::TCounterPtr GeneralFetchBytes;
    NMonitoring::TDynamicCounters::TCounterPtr GeneralFetchCount;

    NMonitoring::TDynamicCounters::TCounterPtr HasResultsAckRequest;
    NMonitoring::TDynamicCounters::TCounterPtr NoResultsAckRequest;
    NMonitoring::TDynamicCounters::TCounterPtr AckWaitingDuration;

    std::vector<NMonitoring::THistogramPtr> ScanDurationByStatus;

    NMonitoring::TDynamicCounters::TCounterPtr NoScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr NoScanIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr LinearScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr LinearScanIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr LogScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr LogScanIntervals;

public:

    std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters> ResourcesSubscriberCounters;

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

    NMonitoring::TDynamicCounters::TCounterPtr BlobsWaitingDuration;
    NMonitoring::THistogramPtr HistogramBlobsWaitingDuration;

    NMonitoring::TDynamicCounters::TCounterPtr BlobsReceivedCount;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsReceivedBytes;

    TScanCounters(const TString& module = "Scan");

    void OnNoScanInterval(const ui32 recordsCount) const {
        NoScanRecords->Add(recordsCount);
        NoScanIntervals->Add(1);
    }

    void OnLinearScanInterval(const ui32 recordsCount) const {
        LinearScanRecords->Add(recordsCount);
        LinearScanIntervals->Add(1);
    }

    void OnLogScanInterval(const ui32 recordsCount) const {
        LogScanRecords->Add(recordsCount);
        LogScanIntervals->Add(1);
    }

    void OnScanDuration(const EStatusFinish status, const TDuration d) const {
        AFL_VERIFY((ui32)status < ScanDurationByStatus.size());
        ScanDurationByStatus[(ui32)status]->Collect(d.MilliSeconds());
    }

    void AckWaitingInfo(const TDuration d) const {
        AckWaitingDuration->Add(d.MicroSeconds());
    }

    void OnBlobReceived(const ui32 size) const {
        BlobsReceivedCount->Add(1);
        BlobsReceivedBytes->Add(size);
    }

    void OnBlobsWaitDuration(const TDuration d) const {
        BlobsWaitingDuration->Add(d.MicroSeconds());
        HistogramBlobsWaitingDuration->Collect(d.MicroSeconds());
    }

    void OnEmptyAck() const {
        NoResultsAckRequest->Add(1);
    }

    void OnNotEmptyAck() const {
        HasResultsAckRequest->Add(1);
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

class TCounterGuard: TNonCopyable {
private:
    std::shared_ptr<TAtomicCounter> Counter;
public:
    TCounterGuard(TCounterGuard&& guard) {
        Counter = guard.Counter;
        guard.Counter = nullptr;
    }

    TCounterGuard(const std::shared_ptr<TAtomicCounter>& counter)
        : Counter(counter)
    {
        AFL_VERIFY(Counter);
        Counter->Inc();
    }
    ~TCounterGuard() {
        if (Counter) {
            AFL_VERIFY(Counter->Dec() >= 0);
        }
    }

};

class TConcreteScanCounters: public TScanCounters {
private:
    using TBase = TScanCounters;
    std::shared_ptr<TAtomicCounter> MergeTasksCount;
    std::shared_ptr<TAtomicCounter> AssembleTasksCount;
    std::shared_ptr<TAtomicCounter> ReadTasksCount;
    std::shared_ptr<TAtomicCounter> ResourcesAllocationTasksCount;
public:
    TScanAggregations Aggregations;

    TCounterGuard GetMergeTasksGuard() const {
        return TCounterGuard(MergeTasksCount);
    }

    TCounterGuard GetReadTasksGuard() const {
        return TCounterGuard(ReadTasksCount);
    }

    TCounterGuard GetResourcesAllocationTasksGuard() const {
        return TCounterGuard(ResourcesAllocationTasksCount);
    }

    TCounterGuard GetAssembleTasksGuard() const {
        return TCounterGuard(AssembleTasksCount);
    }

    bool InWaiting() const {
        return MergeTasksCount->Val() || AssembleTasksCount->Val() || ReadTasksCount->Val() || ResourcesAllocationTasksCount->Val();
    }

    void OnBlobsWaitDuration(const TDuration d, const TDuration fullScanDuration) const {
        TBase::OnBlobsWaitDuration(d);
        Aggregations.OnBlobWaitingDuration(d, fullScanDuration);
    }

    TConcreteScanCounters(const TScanCounters& counters)
        : TBase(counters)
        , MergeTasksCount(std::make_shared<TAtomicCounter>())
        , AssembleTasksCount(std::make_shared<TAtomicCounter>())
        , ReadTasksCount(std::make_shared<TAtomicCounter>())
        , ResourcesAllocationTasksCount(std::make_shared<TAtomicCounter>())
        , Aggregations(TBase::BuildAggregations())
    {

    }
};

}
