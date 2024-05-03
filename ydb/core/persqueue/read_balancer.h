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

    void Handle(TEvPersQueue::TEvWakeupClient::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvDescribe::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvRegisterReadSession::TPtr &ev, const TActorContext& ctx);

    void HandleOnInit(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr &ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvCheckACL::TPtr&, const TActorContext&);
    void Handle(TEvPersQueue::TEvGetPartitionIdForWrite::TPtr&, const TActorContext&);

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&);

    void Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx); // from Partition/PQ
    void Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx); // from ReadSession
    void Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx); // from ReadSession

    TStringBuilder GetPrefix() const;

    TActorId GetPipeClient(const ui64 tabletId, const TActorContext&);
    void RequestTabletIfNeeded(const ui64 tabletId, const TActorContext&, bool pipeReconnected = false);
    void ClosePipe(const ui64 tabletId, const TActorContext&);
    void CheckStat(const TActorContext&);
    void UpdateCounters(const TActorContext&);

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

    void Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvStatsWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);

    void RegisterSession(const TActorId& pipe, const TActorContext& ctx);
    void UnregisterSession(const TActorId& pipe, const TActorContext& ctx);
    void RebuildStructs();
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
        NKikimrPQ::EConsumerScalingSupport ScalingSupport;

        std::vector<::NMonitoring::TDynamicCounters::TCounterPtr> AggregatedCounters;
        THolder<TTabletLabeledCountersBase> Aggr;
    };

    std::unordered_map<TString, TConsumerInfo> Consumers;

    ui64 TxId;
    ui32 NumActiveParts;

    std::vector<TActorId> WaitingResponse;
    std::vector<TEvPersQueue::TEvCheckACL::TPtr> WaitingACLRequests;
    std::vector<TEvPersQueue::TEvDescribe::TPtr> WaitingDescribeRequests;

    enum EPartitionState {
        EPS_FREE = 0,
        EPS_ACTIVE = 1
    };

    struct TPartitionInfo {
        ui64 TabletId;
        EPartitionState State;
        TActorId Session;
        ui32 GroupId;
        NSchemeShard::TTopicTabletInfo::TKeyRange KeyRange;

        void Unlock() { Session = TActorId(); State = EPS_FREE; };
        void Lock(const TActorId& session) { Session = session; State = EPS_ACTIVE; }
    };

    std::unordered_map<ui32, TPartitionInfo> PartitionsInfo;
    std::unordered_map<ui32, std::vector<ui32>> GroupsInfo;

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
    bool NoGroupsInBase;

private:
    struct TClientInfo;

    struct TReadingPartitionStatus {
        // Client had commited rad offset equals EndOffset of the partition
        bool Commited = false;
        // ReadSession reach EndOffset of the partition
        bool ReadingFinished = false;
        // ReadSession connected with new SDK with garantee of read order
        bool ScaleAwareSDK = false;
        // ReadSession reach EndOffset of the partition by first request
        bool StartedReadingFromEndOffset = false;

        size_t Iteration = 0;
        ui64 Cookie = 0;

        TActorId LastPipe;

        // Generation of PQ-tablet and cookie for synchronization of commit information.
        ui32 PartitionGeneration;
        ui64 PartitionCookie;

        // Return true if the reading of the partition has been finished and children's partitions are readable.
        bool IsFinished() const;
        // Return true if children's partitions can't be balance separately.
        bool NeedReleaseChildren() const;
        bool BalanceToOtherPipe() const;

        // Called when reading from a partition is started.
        // Return true if the reading of the partition has been finished before.
        bool StartReading();
        // Called when reading from a partition is stopped.
        // Return true if children's partitions can't be balance separately.
        bool StopReading();

        // Called when the partition is inactive and commited offset is equal to EndOffset.
        // Return true if the commited status changed.
        bool SetCommittedState(ui32 generation, ui64 cookie);
        // Called when the partition reading finished.
        // Return true if the reading status changed.
        bool SetFinishedState(bool scaleAwareSDK, bool startedReadingFromEndOffset);
        // Called when the parent partition is reading.
        bool Reset();
    };

    struct TSessionInfo {
        TSessionInfo(const TString& session, const TActorId sender, const TString& clientNode, ui32 proxyNodeId, TInstant ts)
            : Session(session)
            , Sender(sender)
            , NumSuspended(0)
            , NumActive(0)
            , NumInactive(0)
            , ClientNode(clientNode)
            , ProxyNodeId(proxyNodeId)
            , Timestamp(ts)
        {}

        TString Session;
        TActorId Sender;
        ui32 NumSuspended;
        ui32 NumActive;
        ui32 NumInactive;

        TString ClientNode;
        ui32 ProxyNodeId;
        TInstant Timestamp;

        void Unlock(bool inactive);
    };

    struct TClientGroupInfo {
        TClientGroupInfo(TClientInfo& clientInfo)
            : ClientInfo(clientInfo) {}

        TClientInfo& ClientInfo;

        TString ClientId;
        TString Topic;
        ui64 TabletId;
        TString Path;
        ui32 Generation = 0;
        ui64 SessionKeySalt = 0;
        ui32* Step = nullptr;

        ui32 Group = 0;

        std::unordered_map<ui32, TPartitionInfo> PartitionsInfo; // partitionId -> info
        std::deque<ui32> FreePartitions;
        std::unordered_map<std::pair<TActorId, ui64>, TSessionInfo> SessionsInfo; //map from ActorID and random value - need for reordering sessions in different topics (groups?)

        std::pair<TActorId, ui64> SessionKey(const TActorId pipe) const;
        bool EraseSession(const TActorId pipe);
        TSessionInfo* FindSession(const TActorId pipe);
        TSessionInfo* FindSession(ui32 partitionId);

        void ScheduleBalance(const TActorContext& ctx);
        void Balance(const TActorContext& ctx);

        void LockPartition(const TActorId pipe, TSessionInfo& sessionInfo, ui32 partition, const TActorContext& ctx);
        void ReleasePartition(const ui32 partitionId, const TActorContext& ctx);
        void ReleasePartition(const TActorId pipe, TSessionInfo& sessionInfo, const ui32 count, const TActorContext& ctx);
        void ReleasePartition(const TActorId pipe, TSessionInfo& sessionInfo, const std::set<ui32>& partitions, const TActorContext& ctx);
        THolder<TEvPersQueue::TEvReleasePartition> MakeEvReleasePartition(const TActorId pipe, const TSessionInfo& sessionInfo, const ui32 count, const std::set<ui32>& partitions);

        void FreePartition(ui32 partitionId);
        void ActivatePartition(ui32 partitionId);
        void InactivatePartition(ui32 partitionId);

        TStringBuilder GetPrefix() const;

        std::tuple<ui32, ui32, ui32> TotalPartitions() const;
        void ReleaseExtraPartitions(ui32 desired, ui32 allowPlusOne, const TActorContext& ctx);
        void LockMissingPartitions(ui32 desired,
                                   ui32 allowPlusOne,
                                   const std::function<bool (ui32 partitionId)> partitionPredicate,
                                   const std::function<ssize_t (const TSessionInfo& sessionInfo)> actualExtractor,
                                   const TActorContext& ctx);

        bool WakeupScheduled = false;
    };

    std::unique_ptr<TPartitionScaleManager> PartitionsScaleManager;

    struct TClientInfo {
        constexpr static ui32 MAIN_GROUP = 0;

        TClientInfo(const TPersQueueReadBalancer& balancer, NKikimrPQ::EConsumerScalingSupport scalingSupport)
            : Balancer(balancer)
            , ScalingSupport_(scalingSupport) {
        }

        const TPersQueueReadBalancer& Balancer;
        const NKikimrPQ::EConsumerScalingSupport ScalingSupport_;

        std::unordered_map<ui32, TClientGroupInfo> ClientGroupsInfo; //map from group to info
        std::unordered_map<ui32, TReadingPartitionStatus> ReadingPartitionStatus; // partitionId->status

        size_t SessionsWithGroup = 0;

        TString ClientId;
        TString Topic;
        ui64 TabletId;
        TString Path;
        ui32 Generation = 0;
        ui32 Step = 0;

        bool ScalingSupport() const;

        void KillSessionsWithoutGroup(const TActorContext& ctx);
        void MergeGroups(const TActorContext& ctx);
        TClientGroupInfo& AddGroup(const ui32 group);
        void FillEmptyGroup(const ui32 group, const std::unordered_map<ui32, TPartitionInfo>& partitionsInfo);
        void AddSession(const ui32 group, const std::unordered_map<ui32, TPartitionInfo>& partitionsInfo,
                        const TActorId& sender, const NKikimrPQ::TRegisterReadSession& record);

        bool ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx);

        TStringBuilder GetPrefix() const;

        void UnlockPartition(ui32 partitionId, const TActorContext& ctx);

        TReadingPartitionStatus& GetPartitionReadingStatus(ui32 partitionId);

        bool IsReadeable(ui32 partitionId) const;
        bool IsFinished(ui32 partitionId) const;
        bool SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie);

        TClientGroupInfo* FindGroup(ui32 partitionId);
    };

    std::unordered_map<TString, TClientInfo> ClientsInfo; //map from userId -> to info

private:
    struct TPipeInfo {
        TPipeInfo()
            : ServerActors(0)
        {}

        TString ClientId;         // The consumer name
        TString Session;
        TActorId Sender;
        std::vector<ui32> Groups; // groups which are reading
        ui32 ServerActors;        // the number of pipes connected from SessionActor to ReadBalancer

        // true if client connected to read from concret partitions
        bool WithGroups() { return !Groups.empty(); }

        void Init(const TString& clientId, const TString& session, const TActorId& sender, const std::vector<ui32>& groups) {
            ClientId = clientId;
            Session = session;
            Sender = sender;
            Groups = groups;
        }
    };

    std::unordered_map<TActorId, TPipeInfo> PipesInfo;

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
            HFunc(TEvPersQueue::TEvWakeupClient, Handle);
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
            HFunc(TEvPersQueue::TEvWakeupClient, Handle);
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

NKikimrPQ::EConsumerScalingSupport DefaultScalingSupport();

}
}
