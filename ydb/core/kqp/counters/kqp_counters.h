#pragma once

#include "kqp_db_counters.h"

#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <ydb/core/util/concurrent_rw_hash.h>
#include <ydb/core/kqp/common/kqp_tx_info.h>
#include <ydb/core/kqp/common/kqp_tx_info.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tx/tx_proxy/mon.h>

#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_counters.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/system/spinlock.h>

namespace NKikimr {
namespace NKqp {

class TKqpCountersBase {
protected:
    struct TYdbTxByKindCounters {
        NMonitoring::THistogramPtr TotalDuration;
        NMonitoring::THistogramPtr ServerDuration;
        NMonitoring::THistogramPtr ClientDuration;
    };

protected:
    void CreateYdbTxKindCounters(TKqpTransactionInfo::EKind kind, const TString& name);
    static void UpdateYdbTxCounters(const TKqpTransactionInfo& txInfo,
        THashMap<TKqpTransactionInfo::EKind, TYdbTxByKindCounters>& txCounters);

    void Init();

    void ReportQueryAction(NKikimrKqp::EQueryAction action);
    void ReportQueryType(NKikimrKqp::EQueryType type);

    void ReportSessionGracefulShutdownHit();
    void ReportSessionShutdownRequest();
    void ReportCreateSession(ui64 requestSize);
    void ReportPingSession(ui64 requestSize);
    void ReportCloseSession(ui64 requestSize);
    void ReportQueryRequest(ui64 requestBytes, ui64 parametersBytes, ui64 queryBytes);
    void ReportCancelQuery(ui64 requestSize);

    void ReportQueryWithRangeScan();
    void ReportQueryWithFullScan();
    void ReportQueryAffectedShards(ui64 shardsCount);
    void ReportQueryReadSets(ui64 readSetsCount);
    void ReportQueryReadBytes(ui64 bytesCount);
    void ReportQueryReadRows(ui64 rowsCount);
    void ReportQueryMaxShardReplySize(ui64 replySize);
    void ReportQueryMaxShardProgramSize(ui64 programSize);

    void ReportResponseStatus(ui64 responseSize, Ydb::StatusIds::StatusCode ydbStatus);
    void ReportResultsBytes(ui64 resultsSize);

    static TString GetIssueName(ui32 issueCode);
    void ReportIssues(THashMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr>& issueCounters,
        const Ydb::Issue::IssueMessage& issue);

    void ReportQueryLatency(NKikimrKqp::EQueryAction action, const TDuration& duration);

    void ReportTransaction(const TKqpTransactionInfo& txInfo);

    void ReportSqlVersion(ui16 sqlVersion);

    void ReportWorkerCreated();
    void ReportWorkerFinished(TDuration lifeSpan);
    void ReportWorkerCleanupLatency(TDuration cleanupTime);
    void ReportWorkerClosedIdle();
    void ReportWorkerClosedError();
    void ReportWorkerClosedRequest();
    void ReportQueriesPerWorker(ui32 queryId);

    void ReportSessionActorCreated();
    void ReportSessionActorFinished(TDuration lifeSpan);
    void ReportSessionActorCleanupLatency(TDuration cleanupTime);
    void ReportSessionActorClosedIdle();
    void ReportSessionActorClosedError();
    void ReportSessionActorClosedRequest();
    void ReportQueriesPerSessionActor(ui32 queryId);

    void ReportProxyForwardedRequest();

    void ReportBeginTransaction(ui32 evictedTx, ui32 currentActiveTx, ui32 currentAbortedTx);

    void ReportTxCreated();
    void ReportTxAborted(ui32 abortedCount);

    void ReportQueryCacheHit(bool hit);
    void ReportCompileStart();
    void ReportCompileFinish();
    void ReportCompileError();
    void ReportCompileRequestCompile();
    void ReportCompileRequestGet();
    void ReportCompileRequestInvalidate();
    void ReportCompileRequestRejected();
    void ReportCompileRequestTimeout();
    void ReportCompileDurations(TDuration duration, TDuration cpuTime);
    void ReportRecompileRequestGet();
    ::NMonitoring::TDynamicCounterPtr GetQueryReplayCounters() const;

protected:
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr KqpGroup;
    ::NMonitoring::TDynamicCounterPtr YdbGroup;
    ::NMonitoring::TDynamicCounterPtr QueryReplayGroup;

    // Requests
    THashMap<NKikimrKqp::EQueryAction, ::NMonitoring::TDynamicCounters::TCounterPtr> QueryActionRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr OtherQueryRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr CloseSessionRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr CreateSessionRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr PingSessionRequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr CancelQueryRequests;

    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbRequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueryBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr ParametersBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbParametersBytes;

    ::NMonitoring::TDynamicCounters::TCounterPtr SqlV0Translations;
    ::NMonitoring::TDynamicCounters::TCounterPtr SqlV1Translations;
    ::NMonitoring::TDynamicCounters::TCounterPtr SqlUnknownTranslations;

    THashMap<NKikimrKqp::EQueryType, ::NMonitoring::TDynamicCounters::TCounterPtr> QueryTypes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OtherQueryTypes;

    ::NMonitoring::TDynamicCounters::TCounterPtr QueriesWithRangeScan;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueriesWithFullScan;
    NMonitoring::THistogramPtr QueryAffectedShardsCount;
    NMonitoring::THistogramPtr QueryReadSetsCount;
    NMonitoring::THistogramPtr QueryReadBytes;
    NMonitoring::THistogramPtr QueryReadRows;
    NMonitoring::THistogramPtr QueryMaxShardReplySize;
    NMonitoring::THistogramPtr QueryMaxShardProgramSize;

    // Request latency
    THashMap<NKikimrKqp::EQueryAction, NMonitoring::THistogramPtr> QueryLatencies;
    NMonitoring::THistogramPtr YdbQueryExecuteLatency;

    // Responses
    ::NMonitoring::TDynamicCounters::TCounterPtr OtherResponses;
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbResponsesLocksInvalidated;

    THashMap<Ydb::StatusIds::StatusCode, ::NMonitoring::TDynamicCounters::TCounterPtr> YdbResponses;
    ::NMonitoring::TDynamicCounters::TCounterPtr OtherYdbResponses;

    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbResponseBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueryResultsBytes;

    // Workers
    NMonitoring::THistogramPtr WorkerLifeSpan;
    NMonitoring::THistogramPtr QueriesPerWorker;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersClosedIdle;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersClosedError;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersClosedRequest;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveWorkers;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionGracefulShutdownHit;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionBalancerShutdowns;
    ::NMonitoring::TDynamicCounters::TCounterPtr ProxyForwardedRequests;

    NMonitoring::THistogramPtr WorkerCleanupLatency;

    // Workers and SessionActors
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbSessionsClosedIdle;
    ::NMonitoring::TDynamicCounters::TCounterPtr YdbSessionsActiveCount;

    // SessionActors
    NMonitoring::THistogramPtr SessionActorLifeSpan;
    NMonitoring::THistogramPtr QueriesPerSessionActor;

    ::NMonitoring::TDynamicCounters::TCounterPtr SessionActorsCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionActorsClosedIdle;
    //::NMonitoring::TDynamicCounters::TCounterPtr YdbSessionActorsClosedIdle;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionActorsClosedError;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionActorsClosedRequest;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveSessionActors;
    NMonitoring::THistogramPtr SessionActorCleanupLatency;

    // Transactions
    ::NMonitoring::TDynamicCounters::TCounterPtr TxCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr TxAborted;
    ::NMonitoring::TDynamicCounters::TCounterPtr TxCommited;
    ::NMonitoring::TDynamicCounters::TCounterPtr TxEvicted;
    NMonitoring::THistogramPtr TxActivePerSession;
    NMonitoring::THistogramPtr TxAbortedPerSession;
    THashMap<TKqpTransactionInfo::EKind, TYdbTxByKindCounters> YdbTxByKind;

    // Compile service
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCacheHits;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCacheMisses;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsCompile;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsInvalidate;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsRejected;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsTimeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileRequestsRecompile;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileTotal;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileActive;
    NMonitoring::THistogramPtr CompileCpuTime;
    NMonitoring::THistogramPtr YdbCompileDuration;
};


class TKqpDbCounters : public NSysView::IDbCounters, public TKqpCountersBase {
    friend class TKqpCounters;

public:
    // per database internal counters, not exposed
    TKqpDbCounters();

    // created in SVP, exposed
    explicit TKqpDbCounters(const ::NMonitoring::TDynamicCounterPtr& externalGroup,
        const ::NMonitoring::TDynamicCounterPtr& internalGroup);

    void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) override;
    void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) override;

private:
    enum ESimpleCounter {
        DB_KQP_SIMPLE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_KQP_SIMPLE_COUNTER_SIZE
    };
    enum ECumulativeCounter {
        DB_KQP_CUMULATIVE_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_KQP_CUMULATIVE_COUNTER_SIZE
    };
    enum EHistogramCounter {
        DB_KQP_HISTOGRAM_COUNTERS_MAP(ENUM_VALUE_GEN_NO_VALUE)
        DB_KQP_HISTOGRAM_COUNTER_SIZE
    };

    struct TDeprecatedCounter {
        void Set(ui64) {}
        ui64 Val() { return 0; }
    };
    TDeprecatedCounter DeprecatedCounter;
};

using TKqpDbCountersPtr = TIntrusivePtr<TKqpDbCounters>;

class TKqpCounters : public TKqpCountersBase, public NYql::NDq::TSpillingCounters {
private:
    struct TTxByKindCounters {
        NMonitoring::THistogramPtr TotalDuration;
        NMonitoring::THistogramPtr ServerDuration;
        NMonitoring::THistogramPtr ClientDuration;
        NMonitoring::THistogramPtr Queries;
    };

private:
    void CreateTxKindCounters(TKqpTransactionInfo::EKind kind, const TString& name);
    static void UpdateTxCounters(const TKqpTransactionInfo& txInfo,
        THashMap<TKqpTransactionInfo::EKind, TTxByKindCounters>& txCounters);

public:
    explicit TKqpCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TActorContext* ctx = nullptr);

    void ReportProxyForwardedRequest(TKqpDbCountersPtr dbCounters);
    void ReportSessionGracefulShutdownHit(TKqpDbCountersPtr dbCounters);
    void ReportSessionShutdownRequest(TKqpDbCountersPtr dbCounters);
    void ReportCreateSession(TKqpDbCountersPtr dbCounters, ui64 requestSize);
    void ReportPingSession(TKqpDbCountersPtr dbCounters, ui64 requestSize);
    void ReportCloseSession(TKqpDbCountersPtr dbCounters, ui64 requestSize);
    void ReportQueryAction(TKqpDbCountersPtr dbCounters, NKikimrKqp::EQueryAction action);
    void ReportQueryType(TKqpDbCountersPtr dbCounters, NKikimrKqp::EQueryType type);
    void ReportQueryRequest(TKqpDbCountersPtr dbCounters, ui64 requestBytes, ui64 parametersBytes, ui64 queryBytes);
    void ReportCancelQuery(TKqpDbCountersPtr dbCounters, ui64 requestSize);

    void ReportResponseStatus(TKqpDbCountersPtr dbCounters, ui64 responseSize, Ydb::StatusIds::StatusCode ydbStatus);
    void ReportResultsBytes(TKqpDbCountersPtr dbCounters, ui64 resultsSize);
    void ReportIssues(TKqpDbCountersPtr dbCounters,
        THashMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr>& issueCounters,
        const Ydb::Issue::IssueMessage& issue);

    void ReportQueryWithRangeScan(TKqpDbCountersPtr dbCounters);
    void ReportQueryWithFullScan(TKqpDbCountersPtr dbCounters);
    void ReportQueryAffectedShards(TKqpDbCountersPtr dbCounters, ui64 shardsCount);
    void ReportQueryReadSets(TKqpDbCountersPtr dbCounters, ui64 readSetsCount);
    void ReportQueryReadBytes(TKqpDbCountersPtr dbCounters, ui64 bytesCount);
    void ReportQueryReadRows(TKqpDbCountersPtr dbCounters, ui64 rowsCount);
    void ReportQueryMaxShardReplySize(TKqpDbCountersPtr dbCounters, ui64 replySize);
    void ReportQueryMaxShardProgramSize(TKqpDbCountersPtr dbCounters, ui64 programSize);

    void ReportQueryLatency(TKqpDbCountersPtr dbCounters,
        NKikimrKqp::EQueryAction action, const TDuration& duration);
    void ReportSqlVersion(TKqpDbCountersPtr dbCounters, ui16 sqlVersion);
    void ReportTransaction(TKqpDbCountersPtr dbCounters, const TKqpTransactionInfo& txInfo);

    void ReportLeaseUpdateLatency(const TDuration& duration);
    void ReportRunActorLeaseUpdateBacklog(const TDuration& duration);

    void ReportWorkerCreated(TKqpDbCountersPtr dbCounters);
    void ReportWorkerFinished(TKqpDbCountersPtr dbCounters, TDuration lifeSpan);
    void ReportWorkerCleanupLatency(TKqpDbCountersPtr dbCounters, TDuration cleanupTime);
    void ReportWorkerClosedIdle(TKqpDbCountersPtr dbCounters);
    void ReportWorkerClosedError(TKqpDbCountersPtr dbCounters);
    void ReportWorkerClosedRequest(TKqpDbCountersPtr dbCounters);
    void ReportQueriesPerWorker(TKqpDbCountersPtr dbCounters, ui32 queryId);

    void ReportSessionActorCreated(TKqpDbCountersPtr dbCounters);
    void ReportSessionActorFinished(TKqpDbCountersPtr dbCounters, TDuration lifeSpan);
    void ReportSessionActorCleanupLatency(TKqpDbCountersPtr dbCounters, TDuration cleanupTime);
    void ReportSessionActorClosedIdle(TKqpDbCountersPtr dbCounters);
    void ReportSessionActorClosedError(TKqpDbCountersPtr dbCounters);
    void ReportSessionActorClosedRequest(TKqpDbCountersPtr dbCounters);
    void ReportQueriesPerSessionActor(TKqpDbCountersPtr dbCounters, ui32 queryId);

    void ReportBeginTransaction(TKqpDbCountersPtr dbCounters,
        ui32 evictedTx, ui32 currentActiveTx, ui32 currentAbortedTx);

    void ReportTxCreated(TKqpDbCountersPtr dbCounters);
    void ReportTxAborted(TKqpDbCountersPtr dbCounters, ui32 abortedCount);

    void ReportQueryCacheHit(TKqpDbCountersPtr dbCounters, bool hit);
    void ReportCompileStart(TKqpDbCountersPtr dbCounters);
    void ReportCompileFinish(TKqpDbCountersPtr dbCounters);
    void ReportCompileError(TKqpDbCountersPtr dbCounters);
    void ReportCompileRequestCompile(TKqpDbCountersPtr dbCounters);
    void ReportCompileRequestGet(TKqpDbCountersPtr dbCounters);
    void ReportCompileRequestInvalidate(TKqpDbCountersPtr dbCounters);
    void ReportCompileRequestRejected(TKqpDbCountersPtr dbCounters);
    void ReportCompileRequestTimeout(TKqpDbCountersPtr dbCounters);
    void ReportCompileDurations(TKqpDbCountersPtr dbCounters, TDuration duration, TDuration cpuTime);
    void ReportRecompileRequestGet(TKqpDbCountersPtr dbCounters);

    const ::NMonitoring::TDynamicCounters::TCounterPtr RecompileRequestGet() const;
    ::NMonitoring::TDynamicCounterPtr GetKqpCounters() const;
    ::NMonitoring::TDynamicCounterPtr GetQueryReplayCounters() const;
    ::NMonitoring::TDynamicCounterPtr GetWorkloadManagerCounters() const;
    const ::NMonitoring::TDynamicCounters::TCounterPtr GetActiveSessionActors() const;
    const ::NMonitoring::TDynamicCounters::TCounterPtr GetTxReplySizeExceededError() const;
    const ::NMonitoring::TDynamicCounters::TCounterPtr GetDataShardTxReplySizeExceededError() const;

    ::NMonitoring::TDynamicCounters::TCounterPtr GetQueryTypeCounter(NKikimrKqp::EQueryType queryType);

    TKqpDbCountersPtr GetDbCounters(const TString& database);
    void RemoveDbCounters(const TString& database);

public:
    ::NMonitoring::TDynamicCounterPtr WorkloadManagerGroup;

    ::NMonitoring::TDynamicCounters::TCounterPtr FullScansExecuted;

    // Lease updates counters
    ::NMonitoring::THistogramPtr LeaseUpdateLatency;
    ::NMonitoring::THistogramPtr RunActorLeaseUpdateBacklog;

    // Transactions
    THashMap<TKqpTransactionInfo::EKind, TTxByKindCounters> TxByKind;
    ::NMonitoring::TDynamicCounters::TCounterPtr TxReplySizeExceededError;
    ::NMonitoring::TDynamicCounters::TCounterPtr DataShardTxReplySizeExceededError;

    // Compile service
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCacheSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCacheBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCacheEvicted;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueueSize;

    // Compile computation pattern service
    ::NMonitoring::TDynamicCounters::TCounterPtr CompiledComputationPatterns;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileComputationPatternsQueueSize;

    // Resource Manager
    ::NMonitoring::TDynamicCounters::TCounterPtr RmComputeActors;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmMemory;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmExternalMemory;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmNotEnoughMemory;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmNotEnoughComputeActors;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmExtraMemAllocs;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmOnStartAllocs;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmExtraMemFree;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmOnCompleteFree;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmInternalError;
    NMonitoring::THistogramPtr RmSnapshotLatency;
    NMonitoring::THistogramPtr NodeServiceStartEventDelivery;
    NMonitoring::THistogramPtr NodeServiceProcessTime;
    NMonitoring::THistogramPtr NodeServiceProcessCancelTime;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmMaxSnapshotLatency;
    ::NMonitoring::TDynamicCounters::TCounterPtr RmNodeNumberInSnapshot;

    // Scan queries counters
    ::NMonitoring::TDynamicCounters::TCounterPtr ScanQueryShardDisconnect;
    ::NMonitoring::TDynamicCounters::TCounterPtr ScanQueryShardResolve;
    NMonitoring::THistogramPtr ScanQueryRateLimitLatency;

    // Iterator reads counters
    ::NMonitoring::TDynamicCounters::TCounterPtr IteratorsShardResolve;
    ::NMonitoring::TDynamicCounters::TCounterPtr IteratorsReadSplits;
    ::NMonitoring::TDynamicCounters::TCounterPtr SentIteratorAcks;
    ::NMonitoring::TDynamicCounters::TCounterPtr SentIteratorCancels;
    ::NMonitoring::TDynamicCounters::TCounterPtr CreatedIterators;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReadActorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReadActorRemoteFirstFetch;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReadActorRemoteFetch;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReadActorAbsentNodeId;
    ::NMonitoring::TDynamicCounters::TCounterPtr StreamLookupActorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReadActorRetries;
    ::NMonitoring::TDynamicCounters::TCounterPtr DataShardIteratorFails;
    ::NMonitoring::TDynamicCounters::TCounterPtr DataShardIteratorMessages;
    ::NMonitoring::TDynamicCounters::TCounterPtr IteratorDeliveryProblems;

    // Scheduler signals
    ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerThrottled;
    ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerCapacity;
    NMonitoring::THistogramPtr ComputeActorExecutions;
    ::NMonitoring::TDynamicCounters::TCounterPtr ThrottledActorsSpuriousActivations;
    NMonitoring::THistogramPtr SchedulerDelays;

    // Sequences counters
    ::NMonitoring::TDynamicCounters::TCounterPtr SequencerActorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr SequencerErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr SequencerOk;

    // Physical tx duration
    NMonitoring::THistogramPtr LiteralTxTotalTimeHistogram;
    NMonitoring::THistogramPtr DataTxTotalTimeHistogram;
    NMonitoring::THistogramPtr ScanTxTotalTimeHistogram;

    TAlignedPagePoolCounters AllocCounters;

    // db counters
    TConcurrentRWHashMap<TString, TKqpDbCountersPtr, 256> DbCounters;
    TActorSystem* ActorSystem = nullptr;
    TActorId DbWatcherActorId;
};

struct TKqpRequestCounters : public TThrRefBase {
    using TPtr = TIntrusivePtr<TKqpRequestCounters>;

    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpDbCounters> DbCounters; // may be null
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
};

} // namespace NKqp
} // namespace NKikimr
