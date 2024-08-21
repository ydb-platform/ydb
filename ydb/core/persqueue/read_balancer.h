#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/partition_scale_manager.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/system/hp_timer.h>
#include "utils.h"

#include <unordered_map>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;

namespace NBalancing {
class TBalancer;
}


class TMetricsTimeKeeper {
public:
    TMetricsTimeKeeper(NMetrics::TResourceMetrics* metrics, const TActorContext& ctx)
        : Ctx(ctx)
        , Metrics(metrics)
    {}

    ~TMetricsTimeKeeper() {
        ui64 counter = ui64(CpuTimer.PassedReset() * 1000000.);
        if (counter && Metrics) {
            Metrics->CPU.Increment(counter);
            Metrics->TryUpdate(Ctx);
        }
    }

private:
    const TActorContext& Ctx;
    NMetrics::TResourceMetrics *Metrics;
    THPTimer CpuTimer;
};


class TPersQueueReadBalancer : public TActor<TPersQueueReadBalancer>, public TTabletExecutedFlat {
    struct TTxPreInit;
    struct TTxInit;
    struct TTxWrite;

    void HandleWakeup(TEvents::TEvWakeup::TPtr&, const TActorContext &ctx);
    void HandleUpdateACL(TEvPersQueue::TEvUpdateACL::TPtr&, const TActorContext &ctx);

    void Die(const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &) override;

    void InitDone(const TActorContext &ctx);

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;
    TString GenerateStat();

    void Handle(TEvPersQueue::TEvDescribe::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvCheckACL::TPtr&, const TActorContext&);
    void Handle(TEvPersQueue::TEvGetPartitionIdForWrite::TPtr&, const TActorContext&);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&);

    void Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);

    // Begin balancing
    void Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx); // from self
    void Handle(TEvPQ::TEvBalanceConsumer::TPtr& ev, const TActorContext& ctx); // from self

    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx); // from Partition/PQ
    void Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx); // from ReadSession
    void Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx); // from ReadSession
    void HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr &ev, const TActorContext& ctx); // from ReadSession
    void Handle(TEvPersQueue::TEvRegisterReadSession::TPtr &ev, const TActorContext& ctx); // from ReadSession
    void Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx);  // from ReadSession

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&);

    void Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr &ev, const TActorContext& ctx);
    // End balancing

    TStringBuilder GetPrefix() const;

    TActorId GetPipeClient(const ui64 tabletId, const TActorContext&);
    void RequestTabletIfNeeded(const ui64 tabletId, const TActorContext&, bool pipeReconnected = false);
    void ClosePipe(const ui64 tabletId, const TActorContext&);
    void CheckStat(const TActorContext&);

    void InitCounters(const TActorContext&);
    void UpdateCounters(const TActorContext&);
    void UpdateConfigCounters();

    void RespondWithACL(
        const TEvPersQueue::TEvCheckACL::TPtr &request,
        const NKikimrPQ::EAccess &access,
        const TString &error,
        const TActorContext &ctx);
    void CheckACL(const TEvPersQueue::TEvCheckACL::TPtr &request, const NACLib::TUserToken& token, const TActorContext &ctx);
    void GetStat(const TActorContext&);
    TEvPersQueue::TEvPeriodicTopicStats* GetStatsEvent();
    void GetACL(const TActorContext&);
    void AnswerWaitingRequests(const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvStatsWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);

    ui64 PartitionReserveSize() {
        return TopicPartitionReserveSize(TabletConfig);
    }

    void StopFindSubDomainPathId();
    void StartFindSubDomainPathId(bool delayFirstRequest = true);

    void StopWatchingSubDomainPathId();
    void StartWatchingSubDomainPathId();


    bool Inited;
    ui64 PathId;
    TString Topic;
    TString Path;
    ui32 Generation;
    int Version;
    ui32 MaxPartsPerTablet;
    ui64 SchemeShardId;
    NKikimrPQ::TPQTabletConfig TabletConfig;
    NACLib::TSecurityObject ACL;
    TInstant LastACLUpdate;


    struct TConsumerInfo {
        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> AggregatedCounters;
        THolder<TTabletLabeledCountersBase> Aggr;
    };

    std::unordered_map<TString, TConsumerInfo> Consumers;

    ui64 TxId;
    ui32 NumActiveParts;

    std::vector<TActorId> WaitingResponse;
    std::vector<TEvPersQueue::TEvCheckACL::TPtr> WaitingACLRequests;
    std::vector<TEvPersQueue::TEvDescribe::TPtr> WaitingDescribeRequests;

public:
    struct TPartitionInfo {
        ui64 TabletId;
    };

private:
    std::unordered_map<ui32, TPartitionInfo> PartitionsInfo;

    struct TTabletInfo {
        ui64 Owner;
        ui64 Idx;
    };

    std::unordered_map<ui64, TTabletInfo> TabletsInfo;
    ui64 MaxIdx;

    ui32 NextPartitionId;
    ui32 NextPartitionIdForWrite;
    ui32 StartPartitionIdForWrite;
    ui32 TotalGroups;

private:

    friend class NBalancing::TBalancer;
    std::unique_ptr<NBalancing::TBalancer> Balancer;

    std::unique_ptr<TPartitionScaleManager> PartitionsScaleManager;

private:

    NMetrics::TResourceMetrics *ResourceMetrics;

    struct TPipeLocation {
        TActorId PipeActor;
        TMaybe<ui64> NodeId;
        TMaybe<ui32> Generation;
    };

    std::unordered_map<ui64, TPipeLocation> TabletPipes;
    std::unordered_set<ui64> PipesRequested;

    bool WaitingForACL;

    std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> AggregatedCounters;

    NMonitoring::TDynamicCounterPtr DynamicCounters;
    NMonitoring::TDynamicCounters::TCounterPtr ActivePartitionCountCounter;
    NMonitoring::TDynamicCounters::TCounterPtr InactivePartitionCountCounter;

    TString DatabasePath;
    TString DatabaseId;
    TString FolderId;
    TString CloudId;

    struct TPartitionStats {
        ui64 DataSize = 0;
        ui64 UsedReserveSize = 0;
        NKikimrPQ::TAggregatedCounters Counters;
        bool HasCounters = false;
    };

    struct TPartitionMetrics {
        ui64 TotalAvgWriteSpeedPerSec = 0;
        ui64 MaxAvgWriteSpeedPerSec = 0;
        ui64 TotalAvgWriteSpeedPerMin = 0;
        ui64 MaxAvgWriteSpeedPerMin = 0;
        ui64 TotalAvgWriteSpeedPerHour = 0;
        ui64 MaxAvgWriteSpeedPerHour = 0;
        ui64 TotalAvgWriteSpeedPerDay = 0;
        ui64 MaxAvgWriteSpeedPerDay = 0;
    };

    struct TAggregatedStats {
        std::unordered_map<ui32, TPartitionStats> Stats;
        std::unordered_map<ui64, ui64> Cookies;

        ui64 TotalDataSize = 0;
        ui64 TotalUsedReserveSize = 0;

        TPartitionMetrics Metrics;
        TPartitionMetrics NewMetrics;

        ui64 Round = 0;
        ui64 NextCookie = 0;

        void AggrStats(ui32 partition, ui64 dataSize, ui64 usedReserveSize);
        void AggrStats(ui64 avgWriteSpeedPerSec, ui64 avgWriteSpeedPerMin, ui64 avgWriteSpeedPerHour, ui64 avgWriteSpeedPerDay);
    };
    TAggregatedStats AggregatedStats;

    struct TTxWritePartitionStats;
    bool TTxWritePartitionStatsScheduled = false;

    ui64 StatsReportRound;

    std::deque<TAutoPtr<TEvPersQueue::TEvRegisterReadSession>> RegisterEvents;
    std::deque<TAutoPtr<TEvPersQueue::TEvPersQueue::TEvUpdateBalancerConfig>> UpdateEvents;

    TActorId FindSubDomainPathIdActor;

    std::optional<TPathId> SubDomainPathId;
    std::optional<TPathId> WatchingSubDomainPathId;

    friend struct TTxWriteSubDomainPathId;
    bool SubDomainOutOfSpace = false;

    TPartitionGraph PartitionGraph;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_READ_BALANCER_ACTOR;
    }

    TPersQueueReadBalancer(const TActorId &tablet, TTabletStorageInfo *info);

    STFUNC(StateInit) {
        auto ctx(ActorContext());
        TMetricsTimeKeeper keeper(ResourceMetrics, ctx);

        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvUpdateBalancerConfig, HandleOnInit);
            HFunc(TEvPersQueue::TEvDescribe, Handle);
            HFunc(TEvPersQueue::TEvRegisterReadSession, HandleOnInit);
            HFunc(TEvPersQueue::TEvGetReadSessionsInfo, Handle);
            HFunc(TEvTabletPipe::TEvServerConnected, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFunc(TEvPersQueue::TEvCheckACL, Handle);
            HFunc(TEvPersQueue::TEvGetPartitionIdForWrite, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            HFunc(TEvPersQueue::TEvGetPartitionsLocation, HandleOnInit);
            default:
                StateInitImpl(ev, SelfId());
                break;
        }
    }

    STFUNC(StateWork) {
        auto ctx(ActorContext());
        TMetricsTimeKeeper keeper(ResourceMetrics, ctx);

        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TEvPersQueue::TEvUpdateACL, HandleUpdateACL);
            HFunc(TEvPersQueue::TEvCheckACL, Handle);
            HFunc(TEvPersQueue::TEvGetPartitionIdForWrite, Handle);
            HFunc(TEvPersQueue::TEvUpdateBalancerConfig, Handle);
            HFunc(TEvPersQueue::TEvDescribe, Handle);
            HFunc(TEvPersQueue::TEvRegisterReadSession, Handle);
            HFunc(TEvPersQueue::TEvGetReadSessionsInfo, Handle);
            HFunc(TEvPersQueue::TEvPartitionReleased, Handle);
            HFunc(TEvTabletPipe::TEvServerConnected, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvPersQueue::TEvStatusResponse, Handle);
            HFunc(TEvPQ::TEvStatsWakeup, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            HFunc(TEvPersQueue::TEvStatus, Handle);
            HFunc(TEvPersQueue::TEvGetPartitionsLocation, Handle);
            HFunc(TEvPQ::TEvReadingPartitionStatusRequest, Handle);
            HFunc(TEvPersQueue::TEvReadingPartitionStartedRequest, Handle);
            HFunc(TEvPersQueue::TEvReadingPartitionFinishedRequest, Handle);
            HFunc(TEvPQ::TEvWakeupReleasePartition, Handle);
            HFunc(TEvPQ::TEvBalanceConsumer, Handle);
            // from PQ
            HFunc(TEvPQ::TEvPartitionScaleStatusChanged, Handle);
            // from TPartitionScaleRequest
            HFunc(TPartitionScaleRequest::TEvPartitionScaleRequestDone, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
                break;
        }
    }

};

TString EncodeAnchor(const TString& value);

}
}
