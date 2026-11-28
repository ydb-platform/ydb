#pragma once

#include "schema.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>
#include <ydb/core/protos/analyze_operation.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/statistics/events.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <ydb/core/util/intrusive_heap.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/intrlist.h>

#include <optional>
#include <random>

namespace NKikimr::NStat {

inline bool IsTerminalAnalyzeState(Ydb::Table::AnalyzeState::State state) {
    return state == Ydb::Table::AnalyzeState::STATE_DONE
        || state == Ydb::Table::AnalyzeState::STATE_CANCELLED
        || state == Ydb::Table::AnalyzeState::STATE_FAILED;
}

class TStatisticsAggregator : public TActor<TStatisticsAggregator>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::STATISTICS_AGGREGATOR;
    }

    TStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info);

private:
    friend struct TStatisticsAggregatorTestAccess;

    using TSSId = ui64;
    using TNodeId = ui32;

    using Schema = TAggregatorSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TStatisticsAggregator>;

    struct TTxInitSchema;
    struct TTxInit;
    struct TTxConfigure;
    struct TTxSchemeShardStats;
    struct TTxAnalyze;
    struct TTxAnalyzeDeadline;
    struct TTxFinishTraversal;
    struct TTxScheduleTraversal;
    struct TTxAnalyzeOpList;
    struct TTxAnalyzeOpGet;
    struct TTxAnalyzeOpCancel;
    struct TTxAnalyzeOpForget;

    struct TEvPrivate {
        enum EEv {
            EvPropagate = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvFastPropagateCheck,
            EvProcessUrgent,
            EvPropagateTimeout,
            EvScheduleTraversal,
            EvAnalyzeDeadline,
            EvScheduleForceTraversal,
            EvReportAnalyzeCounters,

            EvEnd
        };

        struct TEvPropagate : public TEventLocal<TEvPropagate, EvPropagate> {};
        struct TEvFastPropagateCheck : public TEventLocal<TEvFastPropagateCheck, EvFastPropagateCheck> {};
        struct TEvProcessUrgent : public TEventLocal<TEvProcessUrgent, EvProcessUrgent> {};
        struct TEvPropagateTimeout : public TEventLocal<TEvPropagateTimeout, EvPropagateTimeout> {};
        struct TEvScheduleTraversal : public TEventLocal<TEvScheduleTraversal, EvScheduleTraversal> {};
        struct TEvScheduleForceTraversal : public TEventLocal<TEvScheduleForceTraversal, EvScheduleForceTraversal> {};
        struct TEvAnalyzeDeadline : public TEventLocal<TEvAnalyzeDeadline, EvAnalyzeDeadline> {};
        struct TEvReportAnalyzeCounters : public TEventLocal<TEvReportAnalyzeCounters, EvReportAnalyzeCounters> {};

    };

private:
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void ApplyConfig(const NKikimrConfig::TAppConfig& config);
    void StartRecovery(const NKikimrConfig::TAppConfig& config);
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;
    void SubscribeForConfigChanges(const TActorContext& ctx);

    NTabletFlatExecutor::ITransaction* CreateTxInitSchema();
    NTabletFlatExecutor::ITransaction* CreateTxInit();

    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);

    void Handle(TEvStatistics::TEvConfigureAggregator::TPtr& ev);
    void Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev);
    void Handle(TEvPrivate::TEvPropagate::TPtr& ev);
    void Handle(TEvStatistics::TEvConnectNode::TPtr& ev);
    void Handle(TEvStatistics::TEvRequestStats::TPtr& ev);
    void Handle(TEvStatistics::TEvConnectSchemeShard::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
    void Handle(TEvPrivate::TEvFastPropagateCheck::TPtr& ev);
    void Handle(TEvStatistics::TEvPropagateStatisticsResponse::TPtr& ev);
    void Handle(TEvPrivate::TEvProcessUrgent::TPtr& ev);
    void Handle(TEvPrivate::TEvPropagateTimeout::TPtr& ev);

    void ProcessRequests(TNodeId nodeId, const std::vector<TSSId>& ssIds);
    void SendStatisticsToNode(TNodeId nodeId, const std::vector<TSSId>& ssIds);
    void PropagateStatistics();
    void PropagateFastStatistics();
    size_t PropagatePart(const std::vector<TNodeId>& nodeIds, const std::vector<TSSId>& ssIds,
        size_t lastSSIndex, bool useSizeLimit, ui64 cookie);
    TDuration GetPropagateInterval();

    void Handle(TEvStatistics::TEvAnalyze::TPtr& ev);
    void Handle(TEvStatistics::TEvStatTableCreationResponse::TPtr& ev);
    void ResolveStatisticsTablePathId();
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    bool IsStatisticsTable(const TPathId& pathId) const;
    void Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeActorResult::TPtr& ev);
    void Handle(TEvPrivate::TEvScheduleTraversal::TPtr& ev);
    void Handle(TEvPrivate::TEvScheduleForceTraversal::TPtr& ev);
    void StartTraversalScheduler();
    bool HasScanUserDependencies = false;
    void RefreshScanUserDependencies();
    void ScheduleAnalyzeDeadlineCheck();
    void Handle(TEvStatistics::TEvAnalyzeStatus::TPtr& ev);
    void Handle(TEvPrivate::TEvAnalyzeDeadline::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeCancel::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeOpListRequest::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeOpGetRequest::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeOpCancelRequest::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeOpForgetRequest::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeActorProgress::TPtr& ev);
    void Handle(TEvPrivate::TEvReportAnalyzeCounters::TPtr& ev);

    void PassAway() final;

    struct TTraversal {
        TPathId PathId;
        TString OperationId; // Empty for background collection.
        TString Database;
        TInstant StartTime;
        ui64 Attempt = 0;

        TActorId AnalyzeActorId;
        TActorId SaveQueryActorId;
        TActorId DeleteQueryActorId;
        std::deque<TStatisticsItem> StatisticsToSave;
        bool Published = false;
        bool WaitingToSave = false;
        bool WaitingToDelete = false;
        bool Finishing = false;
    };

    struct TTraversalReply {
        TActorId Recipient;
        TString OperationId;
    };

    void ExpireAnalyzeOperations(NIceDb::TNiceDb& db, TInstant now, std::vector<TTraversalReply>& expired);
    void ReplyToExpiredAnalyzeOperations(const TActorContext& ctx, const std::vector<TTraversalReply>& expired);

    void InitializeStatisticsTable();
    void SaveStatisticsToTable(TTraversal& traversal);
    void DeleteStatisticsFromTable(TTraversal& traversal);
    void DispatchFinishTraversalTx(TTraversal& traversal,
        NKikimrStat::TEvAnalyzeResponse::EStatus status, NYql::TIssues issues = {});

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);
    void PersistTraversal(NIceDb::TNiceDb& db, const TTraversal& traversal);
    void DeletePersistedTraversal(NIceDb::TNiceDb& db, TPathId pathId);
    ui64 AllocateTraversalAttempt();
    void StartTraversal(const TActorContext& ctx, TTraversal& traversal);
    bool HasActiveOperation(const TString& operationId) const;
    void DiscardOperationTraversals(const TString& operationId, NIceDb::TNiceDb& db);
    void RemoveTraversal(NIceDb::TNiceDb& db, TTraversal& traversal, bool succeeded);
    TTraversal* FindTraversalByActor(TActorId actor, TActorId TTraversal::* field);
    void ScheduleTraversalsForScan(NIceDb::TNiceDb& db, const TActorContext& ctx, bool allowBackground,
        std::vector<std::pair<TPathId, ui64>>& starts);
    TTraversalReply FinishTraversal(NIceDb::TNiceDb& db, TTraversal& traversal,
        NKikimrStat::TEvAnalyzeResponse::EStatus status, const NYql::TIssues& issues);

    struct TBaseStatisticsTotals {
        ui64 RowCount = 0;
        ui64 BytesSize = 0;
    };

    static TBaseStatisticsTotals CalculateBaseStatisticsTotals(const NKikimrStat::TSchemeShardStats& stats);
    void RebuildBaseStatisticsCounters();
    void ReportBaseStatisticsCounters();
    void ReportAnalyzeCounters();
    void InitAnalyzeCounters();

    // Per-table change counters parsed from BaseStatistics.
    struct TChangeCounters {
        ui64 RowUpdates = 0;
        ui64 RowDeletes = 0;
        ui64 RowCount = 0;
    };

    struct TTableBaseStats {
        TChangeCounters Counters;
        std::optional<ui64> BytesSize;
    };

    bool IsChangeRatioAboveThreshold(
        const TChangeCounters& lastAnalyze, const TChangeCounters& current) const;
    TTableBaseStats GetTableBaseStats(const TPathId& pathId, TSSId schemeShardId);

    std::optional<bool> IsKnownTable(const TPathId& pathId) const;

    static constexpr TDuration AnalyzeOpHistoryRetention = TDuration::Days(7);

    STFUNC(StateInit) {
        namespace NConfigEvents = NConsole::TEvConfigsDispatcher;
        if (ev->GetTypeRewrite() == NConfigEvents::EvGetConfigResponse) {
            StartRecovery(*ev->Get<NConfigEvents::TEvGetConfigResponse>()->Config);
            return;
        }
        if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType &&
                ev->Get<TEvents::TEvUndelivered>()->SourceType == NConfigEvents::EvGetConfigRequest) {
            StartRecovery(NKikimrConfig::TAppConfig::default_instance());
            return;
        }
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig)
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig)
            hFunc(TEvStatistics::TEvConfigureAggregator, Handle);
            hFunc(TEvStatistics::TEvSchemeShardStats, Handle);
            hFunc(TEvPrivate::TEvPropagate, Handle);
            hFunc(TEvStatistics::TEvConnectNode, Handle);
            hFunc(TEvStatistics::TEvRequestStats, Handle);
            hFunc(TEvStatistics::TEvConnectSchemeShard, Handle);
            hFunc(TEvTabletPipe::TEvServerConnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            hFunc(TEvPrivate::TEvFastPropagateCheck, Handle);
            hFunc(TEvStatistics::TEvPropagateStatisticsResponse, Handle);
            hFunc(TEvPrivate::TEvProcessUrgent, Handle);
            hFunc(TEvPrivate::TEvPropagateTimeout, Handle);

            hFunc(TEvStatistics::TEvAnalyze, Handle);
            hFunc(TEvStatistics::TEvStatTableCreationResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvSaveStatisticsQueryResponse, Handle);
            hFunc(TEvStatistics::TEvDeleteStatisticsQueryResponse, Handle);
            hFunc(TEvStatistics::TEvAnalyzeActorResult, Handle);
            hFunc(TEvPrivate::TEvScheduleTraversal, Handle);
            hFunc(TEvPrivate::TEvScheduleForceTraversal, Handle);
            hFunc(TEvStatistics::TEvAnalyzeStatus, Handle);
            hFunc(TEvPrivate::TEvAnalyzeDeadline, Handle);
            hFunc(TEvStatistics::TEvAnalyzeCancel, Handle);
            hFunc(TEvStatistics::TEvAnalyzeOpListRequest, Handle);
            hFunc(TEvStatistics::TEvAnalyzeOpGetRequest, Handle);
            hFunc(TEvStatistics::TEvAnalyzeOpCancelRequest, Handle);
            hFunc(TEvStatistics::TEvAnalyzeOpForgetRequest, Handle);
            hFunc(TEvStatistics::TEvAnalyzeActorProgress, Handle);
            hFunc(TEvPrivate::TEvReportAnalyzeCounters, Handle);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    YDB_LOG_CRIT_CTX_COMP(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS, "TStatisticsAggregator StateWork unexpected event",
                        {"eventType", ev->GetTypeRewrite()});
                }
        }
    }

private:
    TString Database;

    std::mt19937_64 RandomGenerator;

    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;

    // Dynamic counters for background ANALYZE monitoring (with status label via GetSubgroup)
    NMonitoring::TDynamicCounters::TCounterPtr BackgroundAnalyzePendingCounter;
    NMonitoring::TDynamicCounters::TCounterPtr BackgroundAnalyzeCompletedCounter;
    NMonitoring::TDynamicCounters::TCounterPtr BackgroundAnalyzeFailedCounter;
    bool AnalyzeCountersDirty = true;
    static constexpr TDuration AnalyzeCountersReportPeriod = TDuration::Seconds(1);

    bool EnableStatistics = false;
    bool EnableColumnStatistics = false;
    bool EnableBackgroundAnalyzeChangeRatio = false;
    bool EnableAnalyzeSampling = false;

    NKikimrConfig::TStatisticsConfig StatisticsConfig;

    static constexpr size_t StatsOptimizeFirstNodesCount = 3; // optimize first nodes - fast propagation
    static constexpr size_t StatsSizeLimitBytes = 2 << 20; // limit for stats size in one message

    static constexpr TDuration FastCheckInterval = TDuration::MilliSeconds(50);

    // Serialized stats for all paths from a single SchemeShard.
    struct TSerializedBaseStats {
        std::shared_ptr<TString> Committed; // Value that is safely persisted in local DB. Can be nullptr.
        std::shared_ptr<TString> Latest; // Value from the latest update.
        TBaseStatisticsTotals CommittedTotals;
        std::optional<THashMap<TPathId, TTableBaseStats>> ParsedTables; // Populated on demand and reset when Latest changes.

        void IndexTables(const NKikimrStat::TSchemeShardStats& stats);
        const THashMap<TPathId, TTableBaseStats>& GetTables();
    };
    std::unordered_map<TSSId, TSerializedBaseStats> BaseStatistics;
    TBaseStatisticsTotals BaseStatisticsTotals;

    std::unordered_map<TSSId, size_t> SchemeShards; // all connected schemeshards
    std::unordered_map<TActorId, TSSId> SchemeShardPipes; // schemeshard pipe servers

    std::unordered_map<TNodeId, size_t> Nodes; // all connected nodes
    std::unordered_map<TActorId, TNodeId> NodePipes; // node pipe servers

    std::unordered_set<TSSId> RequestedSchemeShards; // all schemeshards that were requested from all nodes

    size_t FastCounter = StatsOptimizeFirstNodesCount;
    bool FastCheckInFlight = false;
    std::unordered_set<TNodeId> FastNodes; // nodes for fast propagation
    std::unordered_set<TSSId> FastSchemeShards; // schemeshards for fast propagation

    ui64 CurPropagationSeq = 0;
    static constexpr ui64 InvalidPropagationSeq = -1; // used as request cookie for fast propagation requests
    bool PropagationInFlight = false;
    std::vector<TNodeId> PropagationNodes;
    std::vector<TSSId> PropagationSchemeShards;
    size_t LastSSIndex = 0;

    std::queue<TEvStatistics::TEvRequestStats::TPtr> PendingRequests;
    bool ProcessUrgentInFlight = false;

    bool IsStatisticsTableCreated = false;
    TPathId StatisticsTablePathId;


    // period for both force and schedule traversals
    static constexpr TDuration TraversalPeriod = TDuration::Seconds(1);
    // A periodic tick or its transaction is pending.
    bool TraversalSchedulerStarted = false;
    // if table traverse time is older, than traserse it on schedule
    static constexpr TDuration ScheduleTraversalPeriod = TDuration::Hours(24);

    struct TScheduleTraversal {
        TPathId PathId;
        ui64 SchemeShardId = 0;
        TInstant LastUpdateTime;
        bool IsColumnTable = false;

        ui64 LastAnalyzeRowUpdates = Max<ui64>();
        ui64 LastAnalyzeRowDeletes = Max<ui64>();

        size_t HeapIndexByTime = -1;

        struct THeapIndexByTime {
            size_t& operator()(TScheduleTraversal& value) const {
                return value.HeapIndexByTime;
            }
        };

        struct TLessByTime {
            bool operator()(const TScheduleTraversal& l, const TScheduleTraversal& r) const {
                return l.LastUpdateTime < r.LastUpdateTime;
            }
        };
    };

    // Select up to limit eligible tables above the change-ratio threshold, oldest first.
    std::vector<const TScheduleTraversal*> FindStaleTables(size_t limit);

    static constexpr TDuration AnalyzeDeadline = TDuration::Days(1);
    static constexpr TDuration AnalyzeDeadlinePeriod = TDuration::Seconds(1);
    bool AnalyzeDeadlineCheckScheduled = false;

    std::unordered_map<TPathId, TTraversal> ActiveTraversals;
    // Interrupted background traversals whose tables disappeared from SchemeShard's schedule.
    std::unordered_set<TPathId> PendingBackgroundChecks;
    TActorId ScanBudgetActorId;
    ui32 AttemptSequence = 0;

    std::unordered_map<TPathId, TScheduleTraversal> ScheduleTraversals;
    std::unordered_map<ui64, std::unordered_set<TPathId>> ScheduleTraversalsBySchemeShard;
    typedef TIntrusiveHeap<TScheduleTraversal, TScheduleTraversal::THeapIndexByTime, TScheduleTraversal::TLessByTime>
        TTraversalsByTime;
    TTraversalsByTime ScheduleTraversalsByTime;


    struct TForceTraversalTable {
        TPathId PathId;
        TVector<ui32> ColumnTags;
        TString Path;            // full table path, persisted in ForceTraversalTables
        double SampleRate = 1.0;
        ui32 ShardsTotal = 0;   // set by TEvAnalyzeActorProgress
        ui32 ShardsDone  = 0;   // incremented per scan completion (current batch)

        enum class EStatus : ui8 {
            None,
            AnalyzeStarted,
            AnalyzeFinished,
            TraversalStarted,
            TraversalFinished,
        };
        EStatus Status = EStatus::None;

        TString GetStatusString() const;
    };
    struct TForceTraversalOperation {
        TString OperationId;
        TString DatabaseName;
        std::vector<TForceTraversalTable> Tables;
        TString Types;
        TActorId ReplyToActorId;
        TInstant CreatedAt;
        // Terminal state (STATE_UNSPECIFIED means non-terminal; live state is derived at read time)
        Ydb::Table::AnalyzeState::State State = Ydb::Table::AnalyzeState::STATE_UNSPECIFIED;
        TInstant EndTime;
        // Issues attached to a terminal operation (cancellation reason, deadline message, scan errors).
        // In-memory only; lost on tablet restart (acceptable: the original requester already received
        // the issues via TEvAnalyzeResponse).
        NYql::TIssues Issues;
    };
    std::list<TForceTraversalOperation> ForceTraversals;

private:
    TForceTraversalOperation* ForceTraversalOperation(const TString& operationId);
    void DeleteForceTraversalOperation(const TString& operationId, NIceDb::TNiceDb& db);

    // ForceTraversals now retains terminal entries as 7-day history; the "INFLIGHT"
    // counters must exclude them so monitoring/alerts based on those counters remain
    // accurate.
    size_t InflightForceTraversalCount() const;
    void RecalcForceTraversalsInflightSizeCounter();
    void RecalcForceTraversalInflightMaxTimeCounter(TInstant now);

    TForceTraversalTable* ForceTraversalTable(const TString& operationId, const TPathId& pathId);
    void UpdateForceTraversalTableStatus(const TForceTraversalTable::EStatus status, const TString& operationId, TStatisticsAggregator::TForceTraversalTable& table, NIceDb::TNiceDb& db);

    // Mark a force-traversal operation as terminal (DONE/CANCELLED).
    // Persists State and EndTime, does NOT remove from ForceTraversals.
    // Idempotent: if the operation is already terminal, returns immediately without overwriting
    // — protects against races (e.g., deadline marks CANCELLED, then a late AnalyzeActor result
    // tries to mark DONE for the same op).
    void MarkForceTraversalOperationFinished(
        const TString& operationId,
        Ydb::Table::AnalyzeState::State state,
        TInstant endTime,
        NIceDb::TNiceDb& db,
        NYql::TIssues issues = {});

    // Populate TAnalyzeOperation proto from in-memory TForceTraversalOperation.
    void FillAnalyzeOperationProto(
        const TForceTraversalOperation& op,
        NKikimrAnalyzeOp::TAnalyzeOperation& proto) const;

    // Build and send an UNSUPPORTED response for a long-running ANALYZE request
    // when EnableAnalyzeLongRunningOperation is off.
    template<typename TResponse>
    void SendAnalyzeLongRunningOpDisabled(const TActorId& recipient, ui64 cookie) {
        auto response = MakeHolder<TResponse>();
        response->Record.SetStatus(Ydb::StatusIds::UNSUPPORTED);
        auto& issue = *response->Record.AddIssues();
        issue.set_severity(NYql::TSeverityIds::S_ERROR);
        issue.set_message("ANALYZE long-running operation is disabled");
        Send(recipient, response.Release(), 0, cookie);
    }
};

} // NKikimr::NStat
