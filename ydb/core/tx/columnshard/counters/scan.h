#pragma once
#include "sub_columns.h"

#include "common/histogram.h"
#include "common/owner.h"

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/resource_subscriber/counters.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/columnshard/resources/memory.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TCompiledGraph;
}

namespace NKikimr::NColumnShard {

class TScanAggregations: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::shared_ptr<NOlap::TMemoryAggregation> ResultsReady;
    std::shared_ptr<NOlap::TMemoryAggregation> RequestedResourcesMemory;
    std::shared_ptr<TValueAggregationClient> ScanDuration;
    std::shared_ptr<TValueAggregationClient> BlobsWaitingDuration;

public:
    TScanAggregations(const TString& moduleId)
        : TBase(moduleId)
        , ResultsReady(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Results/Ready"))
        , RequestedResourcesMemory(std::make_shared<NOlap::TMemoryAggregation>(moduleId, "InFlight/Resources/Requested"))
        , ScanDuration(TBase::GetValueAutoAggregationsClient("ScanDuration"))
        , BlobsWaitingDuration(TBase::GetValueAutoAggregationsClient("BlobsWaitingDuration")) {
    }

    std::shared_ptr<NOlap::TMemoryAggregation> GetRequestedResourcesMemory() const {
        return RequestedResourcesMemory;
    }

    void OnBlobWaitingDuration(const TDuration d, const TDuration fullScanDuration) const {
        BlobsWaitingDuration->Add(d.MicroSeconds());
        ScanDuration->SetValue(fullScanDuration.MicroSeconds());
    }

    const std::shared_ptr<NOlap::TMemoryAggregation>& GetResultsReady() const {
        return ResultsReady;
    }
};

class TScanCounters: public TCommonCountersOwner {
public:
    enum class EIntervalStatus {
        Undefined = 0,
        WaitResources,
        WaitSources,
        WaitMergerStart,
        WaitMergerContinue,
        WaitPartialReply,

        COUNT
    };

    enum class EStatusFinish {
        Success /* "Success" */ = 0,
        ConveyorInternalError /* "ConveyorInternalError" */,
        ExternalAbort /* "ExternalAbort" */,
        IteratorInternalErrorScan /* "IteratorInternalErrorScan" */,
        IteratorInternalErrorResult /* "IteratorInternalErrorResult" */,
        Deadline /* "Deadline" */,
        UndeliveredEvent /* "UndeliveredEvent" */,
        CannotAddInFlight /* "CannotAddInFlight" */,
        ProblemOnStart /*ProblemOnStart*/,

        COUNT
    };

    class TScanIntervalState {
    private:
        std::vector<NMonitoring::TDynamicCounters::TCounterPtr> ValuesByStatus;

    public:
        TScanIntervalState(const TScanCounters& counters) {
            ValuesByStatus.resize((ui32)EIntervalStatus::COUNT);
            for (auto&& i : GetEnumAllValues<EIntervalStatus>()) {
                if (i == EIntervalStatus::COUNT) {
                    continue;
                }
                ValuesByStatus[(ui32)i] = counters.CreateSubGroup("status", ::ToString(i)).GetValue("Intervals/Count");
            }
        }
        void Add(const EIntervalStatus status) const {
            AFL_VERIFY((ui32)status < ValuesByStatus.size());
            ValuesByStatus[(ui32)status]->Add(1);
        }
        void Remove(const EIntervalStatus status) const {
            AFL_VERIFY((ui32)status < ValuesByStatus.size());
            ValuesByStatus[(ui32)status]->Sub(1);
        }
    };

    class TScanIntervalStateGuard {
    private:
        EIntervalStatus Status = EIntervalStatus::Undefined;
        const std::shared_ptr<TScanIntervalState> BaseCounters;

    public:
        TScanIntervalStateGuard(const std::shared_ptr<TScanIntervalState>& baseCounters)
            : BaseCounters(baseCounters) {
            BaseCounters->Add(Status);
        }

        ~TScanIntervalStateGuard() {
            BaseCounters->Remove(Status);
        }

        void SetStatus(const EIntervalStatus status) {
            BaseCounters->Remove(Status);
            Status = status;
            BaseCounters->Add(Status);
        }
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
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> ScansFinishedByStatus;

    NMonitoring::TDynamicCounters::TCounterPtr NoScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr NoScanIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr LinearScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr LinearScanIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr LogScanRecords;
    NMonitoring::TDynamicCounters::TCounterPtr LogScanIntervals;
    std::shared_ptr<TScanIntervalState> ScanIntervalState;

    NMonitoring::THistogramPtr HistogramIntervalMemoryRequiredOnFail;
    NMonitoring::THistogramPtr HistogramIntervalMemoryReduceSize;
    NMonitoring::THistogramPtr HistogramIntervalMemoryRequiredAfterReduce;
    NMonitoring::TDynamicCounters::TCounterPtr NotIndexBlobs;
    NMonitoring::TDynamicCounters::TCounterPtr RecordsAcceptedByIndex;
    NMonitoring::TDynamicCounters::TCounterPtr RecordsDeniedByIndex;
    std::shared_ptr<TSubColumnCounters> SubColumnCounters;

public:
    const std::shared_ptr<TSubColumnCounters>& GetSubColumns() const {
        AFL_VERIFY(SubColumnCounters);
        return SubColumnCounters;
    }

    void OnNotIndexBlobs() const {
        NotIndexBlobs->Add(1);
    }
    void OnAcceptedByIndex(const ui32 recordsCount) const {
        RecordsAcceptedByIndex->Add(recordsCount);
    }
    void OnDeniedByIndex(const ui32 recordsCount) const {
        RecordsDeniedByIndex->Add(recordsCount);
    }
    NMonitoring::TDynamicCounters::TCounterPtr AcceptedByIndex;
    NMonitoring::TDynamicCounters::TCounterPtr DeniedByIndex;

    TScanIntervalStateGuard CreateIntervalStateGuard() const {
        return TScanIntervalStateGuard(ScanIntervalState);
    }

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

    NMonitoring::TDynamicCounters::TCounterPtr ProcessedSourceCount;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessedSourceRawBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessedSourceRecords;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessedSourceEmptyCount;
    NMonitoring::THistogramPtr HistogramFilteredResultCount;

    TScanCounters(const TString& module = "Scan");

    void OnSourceFinished(const ui32 recordsCount, const ui64 rawBytes, const ui32 filteredRecordsCount) const {
        ProcessedSourceCount->Add(1);
        ProcessedSourceRawBytes->Add(rawBytes);
        ProcessedSourceRecords->Add(recordsCount);
        HistogramFilteredResultCount->Collect(filteredRecordsCount);
        if (!filteredRecordsCount) {
            ProcessedSourceEmptyCount->Add(1);
        }
    }

    void OnOptimizedIntervalMemoryFailed(const ui64 memoryRequired) const {
        HistogramIntervalMemoryRequiredOnFail->Collect(memoryRequired / (1024.0 * 1024.0 * 1024.0));
    }

    void OnOptimizedIntervalMemoryReduced(const ui64 memoryReduceVolume) const {
        HistogramIntervalMemoryReduceSize->Collect(memoryReduceVolume / (1024.0 * 1024.0 * 1024.0));
    }

    void OnOptimizedIntervalMemoryRequired(const ui64 memoryRequired) const {
        HistogramIntervalMemoryRequiredAfterReduce->Collect(memoryRequired / (1024.0 * 1024.0));
    }

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

    void OnScanFinished(const EStatusFinish status, const TDuration d) const {
        AFL_VERIFY((ui32)status < ScanDurationByStatus.size());
        ScanDurationByStatus[(ui32)status]->Collect(d.MilliSeconds());
        ScansFinishedByStatus[(ui32)status]->Add(1);
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

    void FillStats(::NKikimrTableStats::TTableStats& output) const;
};

class TCounterGuard: TMoveOnly {
private:
    std::shared_ptr<TAtomicCounter> Counter;

public:
    TCounterGuard(TCounterGuard&& guard) {
        Counter = guard.Counter;
        guard.Counter = nullptr;
    }

    TCounterGuard(const std::shared_ptr<TAtomicCounter>& counter)
        : Counter(counter) {
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
    std::shared_ptr<TAtomicCounter> FetchAccessorsCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> FetchBlobsCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> MergeTasksCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> AssembleTasksCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> ReadTasksCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> ResourcesAllocationTasksCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> ResultsForSourceCount = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> ResultsForReplyGuard = std::make_shared<TAtomicCounter>();
    std::shared_ptr<TAtomicCounter> TotalExecutionDurationUs = std::make_shared<TAtomicCounter>();
    THashMap<ui32, std::shared_ptr<TAtomicCounter>> SkipNodesCount;
    THashMap<ui32, std::shared_ptr<TAtomicCounter>> ExecuteNodesCount;

public:
    TScanAggregations Aggregations;

    void OnSkipGraphNode(const ui32 nodeId) const {
        if (SkipNodesCount.size()) {
            auto it = SkipNodesCount.find(nodeId);
            AFL_VERIFY(it != SkipNodesCount.end());
            it->second->Inc();
        }
    }

    void OnExecuteGraphNode(const ui32 nodeId) const {
        if (ExecuteNodesCount.size()) {
            auto it = ExecuteNodesCount.find(nodeId);
            AFL_VERIFY(it != ExecuteNodesCount.end());
            it->second->Inc();
        }
    }

    void AddExecutionDuration(const TDuration d) const {
        TotalExecutionDurationUs->Add(d.MicroSeconds());
    }

    TDuration GetExecutionDuration() const {
        return TDuration::MicroSeconds(TotalExecutionDurationUs->Val());
    }

    TString DebugString() const {
        return TStringBuilder() << "FetchAccessorsCount:" << FetchAccessorsCount->Val() << ";"
                                << "FetchBlobsCount:" << FetchBlobsCount->Val() << ";"
                                << "MergeTasksCount:" << MergeTasksCount->Val() << ";"
                                << "AssembleTasksCount:" << AssembleTasksCount->Val() << ";"
                                << "ReadTasksCount:" << ReadTasksCount->Val() << ";"
                                << "ResourcesAllocationTasksCount:" << ResourcesAllocationTasksCount->Val() << ";"
                                << "ResultsForSourceCount:" << ResultsForSourceCount->Val() << ";"
                                << "ResultsForReplyGuard:" << ResultsForReplyGuard->Val() << ";";
    }

    TCounterGuard GetResultsForReplyGuard() const {
        return TCounterGuard(ResultsForReplyGuard);
    }

    TCounterGuard GetFetcherAcessorsGuard() const {
        return TCounterGuard(FetchAccessorsCount);
    }

    TCounterGuard GetFetchBlobsGuard() const {
        return TCounterGuard(FetchBlobsCount);
    }

    TCounterGuard GetResultsForSourceGuard() const {
        return TCounterGuard(ResultsForSourceCount);
    }

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
        return MergeTasksCount->Val() || AssembleTasksCount->Val() || ReadTasksCount->Val() || ResourcesAllocationTasksCount->Val() ||
               FetchAccessorsCount->Val() || ResultsForSourceCount->Val() || FetchBlobsCount->Val() || ResultsForReplyGuard->Val();
    }

    const THashMap<ui32, std::shared_ptr<TAtomicCounter>>& GetSkipStats() const {
        return SkipNodesCount;
    }

    const THashMap<ui32, std::shared_ptr<TAtomicCounter>>& GetExecutionStats() const {
        return ExecuteNodesCount;
    }

    void OnBlobsWaitDuration(const TDuration d, const TDuration fullScanDuration) const {
        TBase::OnBlobsWaitDuration(d);
        Aggregations.OnBlobWaitingDuration(d, fullScanDuration);
    }

    TConcreteScanCounters(const TScanCounters& counters, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program);
};

}   // namespace NKikimr::NColumnShard
