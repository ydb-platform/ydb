#pragma once

#include "partition_scale_manager.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <util/system/hp_timer.h>

#include <unordered_map>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;

namespace NBalancing {
class TBalancer;
class TMLPBalancer;
}

class TTopicMetricsHandler;

struct TDatabaseInfo {
    TString DatabasePath;
    TString DatabaseId;
    TString FolderId;
    TString CloudId;
};


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


class TPersQueueReadBalancer : public TActor<TPersQueueReadBalancer>,
                               public TTabletExecutedFlat {
    struct TTxPreInit;
    struct TTxInit;
    struct TTxWrite;

    void HandleWakeup(TEvents::TEvWakeup::TPtr&, const TActorContext &ctx);

    void Die(const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &) override;

    void InitDone(const TActorContext &ctx);

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;
    TString GenerateStat();

    void HandleOnInit(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);

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

    // Begin kafka integration
    void Handle(TEvPersQueue::TEvBalancingSubscribe::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvBalancingUnsubscribe::TPtr &ev, const TActorContext& ctx);
    // End kafka integration

    TStringBuilder LogPrefix() const;

    TActorId GetPipeClient(const ui64 tabletId, const TActorContext&);
    void RequestTabletIfNeeded(const ui64 tabletId, const TActorContext&, bool pipeReconnected = false);
    void ClosePipe(const ui64 tabletId, const TActorContext&);
    void CheckStat(const TActorContext&);

    void InitCounters(const TActorContext&);
    void UpdateCounters(const TActorContext&);
    void UpdateConfigCounters();

    void GetStat(const TActorContext&);
    TEvPersQueue::TEvPeriodicTopicStats* GetStatsEvent();
    void AnswerWaitingRequests(const TActorContext& ctx);

    void BroadcastPartitionError(const TString& message, NKikimrServices::EServiceKikimr service, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvStatsWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPConsumerStatus::TPtr&);

    ui64 PartitionReserveSize() {
        return TopicPartitionReserveSize(TabletConfig);
    }

    void StopFindSubDomainPathId();
    void StartFindSubDomainPathId(bool delayFirstRequest = true);

    void StopWatchingSubDomainPathId();
    void StartWatchingSubDomainPathId();

    void ProcessPendingMLPGetPartitionRequests(const TActorContext& ctx);
    void UpdateActivePartitions();

    bool Inited;
    ui64 PathId;
    TString Topic;
    TString Path;
    ui32 Generation;
    int Version;
    ui32 MaxPartsPerTablet;
    ui64 SchemeShardId;
    NKikimrPQ::TPQTabletConfig TabletConfig;

    ui64 TxId;
    ui32 NumActiveParts;

    std::vector<TActorId> WaitingResponse;

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

    friend class NBalancing::TMLPBalancer;
    std::unique_ptr<NBalancing::TMLPBalancer> MLPBalancer;

    std::unique_ptr<TPartitionScaleManager> PartitionsScaleManager;
    TActorId MirrorTopicDescriberActorId;

private:

    NMetrics::TResourceMetrics *ResourceMetrics;

    struct TPipeLocation {
        TActorId PipeActor;
        TMaybe<ui64> NodeId;
        TMaybe<ui32> Generation;
    };

    std::unordered_map<ui64, TPipeLocation> TabletPipes;
    std::unordered_set<ui64> PipesRequested;

    TDatabaseInfo DatabaseInfo;

    std::unique_ptr<TTopicMetricsHandler> TopicMetricsHandler;

    struct TStatsRequestTracker {
        std::unordered_map<ui64, ui64> Cookies;

        ui64 Round = 0;
        ui64 NextCookie = 0;
        bool StatsReceived = false;
    };
    TStatsRequestTracker StatsRequestTracker;

    struct TTxWritePartitionStats;
    bool TTxWritePartitionStatsScheduled = false;

    ui64 StatsReportRound;

    std::deque<TAutoPtr<TEvPersQueue::TEvRegisterReadSession>> RegisterEvents;
    std::deque<TAutoPtr<TEvPersQueue::TEvUpdateBalancerConfig>> UpdateEvents;
    std::deque<TEvPQ::TEvMLPGetPartitionRequest::TPtr> PendingMLPGetPartitionRequests;

    TActorId FindSubDomainPathIdActor;

    std::optional<TPathId> SubDomainPathId;
    std::optional<TPathId> WatchingSubDomainPathId;

    friend struct TTxWriteSubDomainPathId;
    bool SubDomainOutOfSpace = false;

    TPartitionGraph PartitionGraph;
    std::vector<ui32> ActivePartitions;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_READ_BALANCER_ACTOR;
    }

    TPersQueueReadBalancer(const TActorId &tablet, TTabletStorageInfo *info);

    STFUNC(StateInit);
    STFUNC(StateWork);
};

TString EncodeAnchor(const TString& value);

}
}
