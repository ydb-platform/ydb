#pragma once

#include "schema.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

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

    struct TEvPrivate {
        enum EEv {
            EvPropagate = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvFastPropagateCheck,
            EvProcessUrgent,
            EvPropagateTimeout,

            EvEnd
        };

        struct TEvPropagate : public TEventLocal<TEvPropagate, EvPropagate> {};
        struct TEvFastPropagateCheck : public TEventLocal<TEvFastPropagateCheck, EvFastPropagateCheck> {};
        struct TEvProcessUrgent : public TEventLocal<TEvProcessUrgent, EvProcessUrgent> {};
        struct TEvPropagateTimeout : public TEventLocal<TEvPropagateTimeout, EvPropagateTimeout> {};
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

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);

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
};

} // NKikimr::NStat
