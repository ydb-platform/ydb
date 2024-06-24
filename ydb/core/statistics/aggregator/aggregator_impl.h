#pragma once

#include "schema.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/util/count_min_sketch.h>
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
    struct TTxScanTable;
    struct TTxNavigate;
    struct TTxResolve;
    struct TTxStatisticsScanResponse;
    struct TTxSaveQueryResponse;
    struct TTxScheduleScan;
    struct TTxDeleteQueryResponse;

    struct TEvPrivate {
        enum EEv {
            EvPropagate = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvFastPropagateCheck,
            EvProcessUrgent,
            EvPropagateTimeout,
            EvScheduleScan,

            EvEnd
        };

        struct TEvPropagate : public TEventLocal<TEvPropagate, EvPropagate> {};
        struct TEvFastPropagateCheck : public TEventLocal<TEvFastPropagateCheck, EvFastPropagateCheck> {};
        struct TEvProcessUrgent : public TEventLocal<TEvProcessUrgent, EvProcessUrgent> {};
        struct TEvPropagateTimeout : public TEventLocal<TEvPropagateTimeout, EvPropagateTimeout> {};
        struct TEvScheduleScan : public TEventLocal<TEvScheduleScan, EvScheduleScan> {};
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

    void Handle(TEvStatistics::TEvScanTable::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev);
    void Handle(TEvDataShard::TEvStatisticsScanResponse::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    void Handle(TEvStatistics::TEvStatTableCreationResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr& ev);
    void Handle(TEvPrivate::TEvScheduleScan::TPtr& ev);
    void Handle(TEvStatistics::TEvGetScanStatus::TPtr& ev);

    void InitializeStatisticsTable();
    void Navigate();
    void Resolve();
    void NextRange();
    void SaveStatisticsToTable();
    void DeleteStatisticsFromTable();

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);
    void PersistCurrentScan(NIceDb::TNiceDb& db);
    void PersistStartKey(NIceDb::TNiceDb& db);
    void PersistLastScanOperationId(NIceDb::TNiceDb& db);

    void ResetScanState(NIceDb::TNiceDb& db);
    void ScheduleNextScan(NIceDb::TNiceDb& db);
    void StartScan(NIceDb::TNiceDb& db, TPathId pathId);
    void FinishScan(NIceDb::TNiceDb& db);


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

            hFunc(TEvStatistics::TEvScanTable, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            hFunc(TEvDataShard::TEvStatisticsScanResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvStatistics::TEvStatTableCreationResponse, Handle);
            hFunc(TEvStatistics::TEvSaveStatisticsQueryResponse, Handle);
            hFunc(TEvStatistics::TEvDeleteStatisticsQueryResponse, Handle);
            hFunc(TEvPrivate::TEvScheduleScan, Handle);
            hFunc(TEvStatistics::TEvGetScanStatus, Handle);

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

    std::unordered_map<TSSId, TString> BaseStats; // schemeshard id -> serialized stats for all paths

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

    //

    TTableId ScanTableId; // stored in local db
    std::unordered_set<TActorId> ReplyToActorIds;

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
    std::deque<TRange> ShardRanges;

    TSerializedCellVec StartKey; // stored in local db

    std::unordered_map<ui32, std::unique_ptr<TCountMinSketch>> CountMinSketches; // stored in local db

    static constexpr TDuration ScanIntervalTime = TDuration::Hours(24);
    static constexpr TDuration ScheduleScanIntervalTime = TDuration::Seconds(1);

    struct TScanTable {
        TPathId PathId;
        ui64 SchemeShardId = 0;
        TInstant LastUpdateTime;

        size_t HeapIndexByTime = -1;

        struct THeapIndexByTime {
            size_t& operator()(TScanTable& value) const {
                return value.HeapIndexByTime;
            }
        };

        struct TLessByTime {
            bool operator()(const TScanTable& l, const TScanTable& r) const {
                return l.LastUpdateTime < r.LastUpdateTime;
            }
        };
    };
    std::unordered_map<TPathId, TScanTable> ScanTables; // stored in local db

    std::unordered_map<ui64, std::unordered_set<TPathId>> ScanTablesBySchemeShard;

    typedef TIntrusiveHeap<TScanTable, TScanTable::THeapIndexByTime, TScanTable::TLessByTime>
        TScanTableQueueByTime;
    TScanTableQueueByTime ScanTablesByTime;

    struct TScanOperation : public TIntrusiveListItem<TScanOperation> {
        ui64 OperationId = 0;
        TPathId PathId;
        std::unordered_set<TActorId> ReplyToActorIds;
    };
    TIntrusiveList<TScanOperation> ScanOperations; // stored in local db
    std::unordered_map<TPathId, TScanOperation> ScanOperationsByPathId;

    ui64 LastScanOperationId = 0; // stored in local db

    TInstant ScanStartTime;
};

} // NKikimr::NStat
