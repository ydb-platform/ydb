#pragma once

#include "schema.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>

#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/minsketch/count_min_sketch.h>
#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/intrlist.h>

#include <random>

namespace NKikimr::NStat {

class TStatisticsAggregator : public TActor<TStatisticsAggregator>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::STATISTICS_AGGREGATOR;
    }

    TStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info, bool forTests);

private:
    using TSSId = ui64;
    using TNodeId = ui32;

    using Schema = TAggregatorSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TStatisticsAggregator>;

    struct TTxInitSchema;
    struct TTxInit;
    struct TTxConfigure;
    struct TTxSchemeShardStats;
    struct TTxAnalyzeTable;
    struct TTxNavigate;
    struct TTxResolve;
    struct TTxDatashardScanResponse;
    struct TTxFinishTraversal;
    struct TTxScheduleTrasersal;
    struct TTxAggregateStatisticsResponse;
    struct TTxResponseTabletDistribution;
    struct TTxAckTimeout;

    struct TEvPrivate {
        enum EEv {
            EvPropagate = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvFastPropagateCheck,
            EvProcessUrgent,
            EvPropagateTimeout,
            EvScheduleTraversal,
            EvRequestDistribution,
            EvResolve,
            EvAckTimeout,

            EvEnd
        };

        struct TEvPropagate : public TEventLocal<TEvPropagate, EvPropagate> {};
        struct TEvFastPropagateCheck : public TEventLocal<TEvFastPropagateCheck, EvFastPropagateCheck> {};
        struct TEvProcessUrgent : public TEventLocal<TEvProcessUrgent, EvProcessUrgent> {};
        struct TEvPropagateTimeout : public TEventLocal<TEvPropagateTimeout, EvPropagateTimeout> {};
        struct TEvScheduleTraversal : public TEventLocal<TEvScheduleTraversal, EvScheduleTraversal> {};
        struct TEvRequestDistribution : public TEventLocal<TEvRequestDistribution, EvRequestDistribution> {};
        struct TEvResolve : public TEventLocal<TEvResolve, EvResolve> {};

        struct TEvAckTimeout : public TEventLocal<TEvAckTimeout, EvAckTimeout> {
            size_t SeqNo = 0;
            explicit TEvAckTimeout(size_t seqNo) {
                SeqNo = seqNo;
            }
        };
    };

private:
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
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
        size_t lastSSIndex, bool useSizeLimit);

    void Handle(TEvStatistics::TEvAnalyze::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);
    void Handle(NStat::TEvStatistics::TEvStatisticsResponse::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void Handle(TEvStatistics::TEvStatTableCreationResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvPrivate::TEvScheduleTraversal::TPtr& ev);
    void Handle(TEvStatistics::TEvAnalyzeStatus::TPtr& ev);
    void Handle(TEvHive::TEvResponseTabletDistribution::TPtr& ev);
    void Handle(TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev);
    void Handle(TEvPrivate::TEvResolve::TPtr& ev);
    void Handle(TEvPrivate::TEvRequestDistribution::TPtr& ev);
    void Handle(TEvStatistics::TEvAggregateKeepAlive::TPtr& ev);
    void Handle(TEvPrivate::TEvAckTimeout::TPtr& ev);

    void InitializeStatisticsTable();
    void Navigate();
    void Resolve();
    void ScanNextDatashardRange();
    void SaveStatisticsToTable();
    void DeleteStatisticsFromTable();

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);
    void PersistTraversal(NIceDb::TNiceDb& db);
    void PersistTraversalOperationIdAndCookie(NIceDb::TNiceDb& db);
    void PersistStartKey(NIceDb::TNiceDb& db);
    void PersistNextForceTraversalOperationId(NIceDb::TNiceDb& db);    
    void PersistGlobalTraversalRound(NIceDb::TNiceDb& db);

    void ResetTraversalState(NIceDb::TNiceDb& db);
    void ScheduleNextTraversal(NIceDb::TNiceDb& db);
    void StartTraversal(NIceDb::TNiceDb& db);
    void FinishTraversal(NIceDb::TNiceDb& db);

    TString LastTraversalWasForceString() const;

    STFUNC(StateInit) {
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
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            hFunc(NStat::TEvStatistics::TEvStatisticsResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvStatistics::TEvStatTableCreationResponse, Handle);
            hFunc(TEvStatistics::TEvSaveStatisticsQueryResponse, Handle);
            hFunc(TEvStatistics::TEvDeleteStatisticsQueryResponse, Handle);
            hFunc(TEvPrivate::TEvScheduleTraversal, Handle);
            hFunc(TEvStatistics::TEvAnalyzeStatus, Handle);
            hFunc(TEvHive::TEvResponseTabletDistribution, Handle);
            hFunc(TEvStatistics::TEvAggregateStatisticsResponse, Handle);
            hFunc(TEvPrivate::TEvResolve, Handle);
            hFunc(TEvPrivate::TEvRequestDistribution, Handle);
            hFunc(TEvStatistics::TEvAggregateKeepAlive, Handle);
            hFunc(TEvPrivate::TEvAckTimeout, Handle);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    LOG_CRIT(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                        "TStatisticsAggregator StateWork unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
                }
        }
    }

private:
    TString Database;

    std::mt19937_64 RandomGenerator;

    bool EnableStatistics = false;
    bool EnableColumnStatistics = false;

    static constexpr size_t StatsOptimizeFirstNodesCount = 3; // optimize first nodes - fast propagation
    static constexpr size_t StatsSizeLimitBytes = 2 << 20; // limit for stats size in one message

    TDuration PropagateInterval;
    TDuration PropagateTimeout;
    static constexpr TDuration FastCheckInterval = TDuration::MilliSeconds(50);

    std::unordered_map<TSSId, TString> BaseStatistics; // schemeshard id -> serialized stats for all paths

    std::unordered_map<TSSId, size_t> SchemeShards; // all connected schemeshards
    std::unordered_map<TActorId, TSSId> SchemeShardPipes; // schemeshard pipe servers

    std::unordered_map<TNodeId, size_t> Nodes; // all connected nodes
    std::unordered_map<TActorId, TNodeId> NodePipes; // node pipe servers

    std::unordered_set<TSSId> RequestedSchemeShards; // all schemeshards that were requested from all nodes

    size_t FastCounter = StatsOptimizeFirstNodesCount;
    bool FastCheckInFlight = false;
    std::unordered_set<TNodeId> FastNodes; // nodes for fast propagation
    std::unordered_set<TSSId> FastSchemeShards; // schemeshards for fast propagation

    bool PropagationInFlight = false;
    std::vector<TNodeId> PropagationNodes;
    std::vector<TSSId> PropagationSchemeShards;
    size_t LastSSIndex = 0;

    std::queue<TEvStatistics::TEvRequestStats::TPtr> PendingRequests;
    bool ProcessUrgentInFlight = false;

    TActorId ForceTraversalReplyToActorId = {};

    bool IsSchemeshardSeen = false;
    bool IsStatisticsTableCreated = false;
    bool PendingSaveStatistics = false;
    bool PendingDeleteStatistics = false;

    std::vector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<TKeyDesc::TColumnOp> Columns;
    std::unordered_map<ui32, TString> ColumnNames;

    struct TRange {
        TSerializedCellVec EndKey;
        ui64 DataShardId = 0;
    };
    std::deque<TRange> DatashardRanges;

    // period for both force and schedule traversals
    static constexpr TDuration TraversalPeriod = TDuration::Seconds(1);
    // if table traverse time is older, than traserse it on schedule
    static constexpr TDuration ScheduleTraversalPeriod = TDuration::Hours(24);

    struct TScheduleTraversal {
        TPathId PathId;
        ui64 SchemeShardId = 0;
        TInstant LastUpdateTime;
        bool IsColumnTable = false;

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

    size_t ResolveRound = 0;
    static constexpr size_t MaxResolveRoundCount = 5;
    static constexpr TDuration ResolveRetryInterval = TDuration::Seconds(1);

    ui64 HiveId = 0;
    std::unordered_set<ui64> TabletsForReqDistribution;

    size_t HiveRequestRound = 0;
    static constexpr size_t MaxHiveRequestRoundCount = 5;
    static constexpr TDuration HiveRetryInterval = TDuration::Seconds(1);

    size_t TraversalRound = 0;
    static constexpr size_t MaxTraversalRoundCount = 5;

    size_t KeepAliveSeqNo = 0;
    static constexpr TDuration KeepAliveTimeout = TDuration::Seconds(3);

    // alternate between forced and scheduled traversals
    bool LastTraversalWasForce = false;

private: // stored in local db
    
    ui64 ForceTraversalOperationId = 0;    
    ui64 ForceTraversalCookie = 0;
    TTableId TraversalTableId; 
    bool TraversalIsColumnTable = false;
    TSerializedCellVec TraversalStartKey;
    TInstant TraversalStartTime;
    ui64 NextForceTraversalOperationId = 0;

    size_t GlobalTraversalRound = 1; 

    std::unordered_map<ui32, std::unique_ptr<TCountMinSketch>> CountMinSketches;   

    std::unordered_map<TPathId, TScheduleTraversal> ScheduleTraversals; 
    std::unordered_map<ui64, std::unordered_set<TPathId>> ScheduleTraversalsBySchemeShard;
    typedef TIntrusiveHeap<TScheduleTraversal, TScheduleTraversal::THeapIndexByTime, TScheduleTraversal::TLessByTime>
        TTraversalsByTime;
    TTraversalsByTime ScheduleTraversalsByTime;

    struct TForceTraversal {
        ui64 OperationId = 0;
        ui64 Cookie = 0;
        TPathId PathId;
        TActorId ReplyToActorId;
    };
    std::list<TForceTraversal> ForceTraversals;
};

} // NKikimr::NStat
