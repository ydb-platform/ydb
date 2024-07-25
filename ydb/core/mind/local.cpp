#include "local.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/scheme_board.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/system/info.h>
#include <util/string/vector.h>

#include <unordered_map>
#include <unordered_set>

template <>
void Out<std::pair<ui64, ui32>>(IOutputStream& out, const std::pair<ui64, ui32>& p) {
    out << '(' << p.first << ',' << p.second << ')';
}

namespace NKikimr {

using NNodeWhiteboard::TTabletId;

class TLocalNodeRegistrar : public TActorBootstrapped<TLocalNodeRegistrar> {
    struct TEvPrivate {
        enum EEv {
            EvRegisterTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvSendTabletMetrics,
            EvUpdateSystemUsage,
            EvLocalDrainTimeout,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        typedef TEventSchedulerEv<EvRegisterTimeout> TEvRegisterTimeout;
        struct TEvSendTabletMetrics : TEventLocal<TEvSendTabletMetrics, EvSendTabletMetrics> {};
        struct TEvUpdateSystemUsage : TEventLocal<TEvUpdateSystemUsage, EvUpdateSystemUsage> {};
        struct TEvLocalDrainTimeout : TEventLocal<TEvLocalDrainTimeout, EvLocalDrainTimeout> {};
    };

    struct TTablet {
        TActorId Tablet;
        ui32 Generation;
        TTabletTypes::EType TabletType;
        NKikimrLocal::EBootMode BootMode;
        ui32 FollowerId;

        TTablet()
            : Tablet()
            , Generation(0)
            , TabletType()
            , BootMode(NKikimrLocal::EBootMode::BOOT_MODE_LEADER)
            , FollowerId(0)
        {}
    };

    struct TTabletEntry : TTablet {
        TInstant From;

        TTabletEntry()
            : From(TInstant::MicroSeconds(0))
        {}
    };

    struct TOnlineTabletEntry : TTabletEntry {
        NKikimrTabletBase::TMetrics ResourceValues;

        TOnlineTabletEntry() = default;
        TOnlineTabletEntry(TTabletEntry& tablet)
            : TTabletEntry(tablet)
        {}
    };

    const TActorId Owner;
    const ui64 HiveId;
    TVector<TSubDomainKey> ServicedDomains;

    TActorId HivePipeClient;
    bool Connected;

    TIntrusivePtr<TLocalConfig> Config;

    TActorId BootQueue;
    ui32 HiveGeneration;

    TActorId KnownHiveLeader;

    using TTabletId = std::pair<ui64, ui32>; // <TTabletId, TFollowerId>
    TInstant StartTime;
    std::unordered_map<TTabletId, TTabletEntry> InbootTablets;
    std::unordered_map<TTabletId, TOnlineTabletEntry> OnlineTablets;
    std::unordered_map<TTabletId, ui32> UpdatedTabletMetrics;
    bool SendTabletMetricsInProgress;
    TInstant SendTabletMetricsTime;
    TDuration LastSendTabletMetricsDuration = {};
    constexpr static TDuration TABLET_METRICS_BATCH_INTERVAL = TDuration::MilliSeconds(5000);
    constexpr static TDuration UPDATE_SYSTEM_USAGE_INTERVAL = TDuration::MilliSeconds(1000);
    constexpr static TDuration DRAIN_NODE_TIMEOUT = TDuration::MilliSeconds(15000);
    ui64 UserPoolUsage = 0; // (usage uS x threads) / sec
    ui64 MemUsage = 0;
    ui64 MemLimit = 0;
    ui64 CpuLimit = 0; // PotentialMaxThreadCount of UserPool
    double NodeUsage = 0;

    bool SentDrainNode = false;
    bool DrainResultReceived = false;
    i32 PrevEstimate = 0;

    TIntrusivePtr<TDrainProgress> LastDrainRequest;

    NKikimrTabletBase::TMetrics ResourceLimit;
    TResourceProfilesPtr ResourceProfiles;
    TSharedQuotaPtr TxCacheQuota;
    ::NMonitoring::TDynamicCounterPtr Counters;

    ::NMonitoring::TDynamicCounters::TCounterPtr CounterStartAttempts;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterFollowerAttempts;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterRestored;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelLocked;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelSSTimeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelRace;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelError;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelBootBSError;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelOutdated;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelBootSSError;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelPoisonPill;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelDemotedBySS;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelBSError;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelInconsistentCommit;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelIsolated;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelDemotedByBS;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterCancelUnknownReason;

    void Die(const TActorContext &ctx) override {
        if (HivePipeClient) {
            if (Connected) {
                NTabletPipe::SendData(ctx, HivePipeClient, new TEvLocal::TEvStatus(TEvLocal::TEvStatus::StatusDead));
            }
            NTabletPipe::CloseClient(ctx, HivePipeClient);
        }
        HivePipeClient = TActorId();

        for (const auto &xpair : OnlineTablets) {
            ctx.Send(xpair.second.Tablet, new TEvents::TEvPoisonPill());
        }
        OnlineTablets.clear();

        for (const auto &xpair : InbootTablets) {
            ctx.Send(xpair.second.Tablet, new TEvents::TEvPoisonPill());
        }
        InbootTablets.clear();

        TActor::Die(ctx);
    }

    void MarkDeadTablet(TTabletId tabletId,
                        ui32 generation,
                        TEvLocal::TEvTabletStatus::EStatus status,
                        TEvTablet::TEvTabletDead::EReason reason,
                        const TActorContext &ctx) {
        if (Connected) { // must be 'connected' check
            NTabletPipe::SendData(ctx, HivePipeClient, new TEvLocal::TEvTabletStatus(status, reason, tabletId, generation));
        }
    }

    void TryToRegister(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::TryToRegister");

        Y_DEBUG_ABORT_UNLESS(!HivePipeClient);

        // pipe client is in use for convenience, real info update come from EvPing
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .MinRetryTime = TDuration::MilliSeconds(100),
            .MaxRetryTime = TDuration::Seconds(5),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        HivePipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, HiveId, pipeConfig));

        THolder<TEvLocal::TEvRegisterNode> request = MakeHolder<TEvLocal::TEvRegisterNode>(HiveId);
        for (auto &domain: ServicedDomains) {
            *request->Record.AddServicedDomains() = NKikimrSubDomains::TDomainKey(domain);
        }
        for (const auto& [tabletType, tabletInfo] : Config->TabletClassInfo) {
            NKikimrLocal::TTabletAvailability* tabletAvailability = request->Record.AddTabletAvailability();
            tabletAvailability->SetType(tabletType);
            if (tabletInfo.MaxCount) {
                tabletAvailability->SetMaxCount(*tabletInfo.MaxCount);
            }
            tabletAvailability->SetPriority(tabletInfo.Priority);
        }
        if (const TString& nodeName = AppData(ctx)->NodeName; !nodeName.Empty()) {
            request->Record.SetName(nodeName);
        }

        NTabletPipe::SendData(ctx, HivePipeClient, request.Release());

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::TryToRegister pipe to hive, pipe:" << HivePipeClient.ToString());
    }

    void HandlePipeDestroyed(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar HandlePipeDestroyed - DISCONNECTED");
        HivePipeClient = TActorId();
        Connected = false;
        TryToRegister(ctx);
        if (SentDrainNode && !DrainResultReceived) {
            LOG_NOTICE_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: drain complete: hive pipe destroyed, hive id: " << HiveId);
            DrainResultReceived = true;
            LastDrainRequest->OnReceive();
            UpdateEstimate();
        }
    }

    void Handle(TEvLocal::TEvEnumerateTablets::TPtr &ev, const TActorContext &ctx) {
        const NKikimrLocal::TEvEnumerateTablets &record = ev->Get()->Record;

        THolder<TEvLocal::TEvEnumerateTabletsResult> result(
            new TEvLocal::TEvEnumerateTabletsResult(NKikimrProto::OK));

        bool isFilteringNeeded = false;
        TTabletTypes::EType tabletType = TTabletTypes::Unknown;
        if (record.HasTabletType()) {
            isFilteringNeeded = true;
            tabletType = record.GetTabletType();
        }

        for (const auto &tablet: OnlineTablets) {
            if (!isFilteringNeeded || tablet.second.TabletType == tabletType) {
                auto *info = result->Record.AddTabletInfo();
                info->SetTabletId(tablet.first.first);
                info->SetFollowerId(tablet.first.second);
                info->SetTabletType(tablet.second.TabletType);
                info->SetBootMode(tablet.second.BootMode);
            }
        }

        ctx.Send(ev->Sender, result.Release());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TEvTabletPipe::TEvClientConnected {"
                    << "TabletId=" << msg->TabletId
                    << " Status=" << msg->Status
                    << " ClientId=" << msg->ClientId);
        if (msg->ClientId != HivePipeClient)
            return;
        if (msg->Status == NKikimrProto::OK) {
            SendTabletMetricsInProgress = false;
            return;
        }
        HandlePipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TEvTabletPipe::TEvClientDestroyed {"
                    << "TabletId=" << msg->TabletId
                    << " ClientId=" << msg->ClientId);
        if (msg->ClientId != HivePipeClient)
            return;
        HandlePipeDestroyed(ctx);
    }

    void FillResourceMaximum(NKikimrTabletBase::TMetrics* record) {
        record->CopyFrom(ResourceLimit);
        if (!record->HasCPU()) {
            if (CpuLimit != 0) {
                record->SetCPU(CpuLimit);
            }
        }
        if (!record->HasMemory()) {
            if (MemLimit != 0) {
                record->SetMemory(MemLimit);
            } else {
                record->SetMemory(NSystemInfo::TotalMemorySize());
            }
        }
    }

    void SendStatusOk(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar SendStatusOk");
        TAutoPtr<TEvLocal::TEvStatus> eventStatus = new TEvLocal::TEvStatus(TEvLocal::TEvStatus::StatusOk);
        auto& record = eventStatus->Record;
        record.SetStartTime(StartTime.GetValue());
        FillResourceMaximum(record.MutableResourceMaximum());
        NTabletPipe::SendData(ctx, HivePipeClient, eventStatus.Release());
    }

    void Handle(TEvLocal::TEvReconnect::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvReconnect");
        const TActorId& sender = ev->Sender;
        const NKikimrLocal::TEvReconnect& record = ev->Get()->Record;
        Y_ABORT_UNLESS(HiveId == record.GetHiveId());
        const ui32 hiveGen = record.GetHiveGeneration();
        if (hiveGen < HiveGeneration) {
            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvReconnect - outdated");
            ctx.Send(sender, new TEvLocal::TEvStatus(TEvLocal::TEvStatus::StatusOutdated));
            return;
        }
        if (HivePipeClient) {
            NTabletPipe::CloseClient(ctx, HivePipeClient); // called automatically HandlePipeDestroyed(ctx);
        } else {
            HandlePipeDestroyed(ctx);
        }
    }

    void Handle(TEvLocal::TEvPing::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvPing");
        const TActorId &sender = ev->Sender;
        const NKikimrLocal::TEvPing &record = ev->Get()->Record;
        Y_ABORT_UNLESS(HiveId == record.GetHiveId());

        const ui32 hiveGen = record.GetHiveGeneration();
        if (hiveGen < HiveGeneration) {
            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvPing - outdated");
            ctx.Send(sender, new TEvLocal::TEvStatus(TEvLocal::TEvStatus::StatusOutdated));
            return;
        }

        if (!HivePipeClient) {
            TryToRegister(ctx);
            return;
        }

        if (!Connected) {
            if (sender != BootQueue) {
                for (const auto &x : InbootTablets)
                    ctx.Send(x.second.Tablet, new TEvents::TEvPoisonPill());
                InbootTablets.clear();
            }

            if (record.GetPurge()) {
                for (const auto &x : OnlineTablets)
                    ctx.Send(x.second.Tablet, new TEvents::TEvPoisonPill());
                OnlineTablets.clear();
            }

            ResourceProfiles = new TResourceProfiles;
            ResourceProfiles->LoadProfiles(record.GetConfig().GetResourceProfiles());

            // we send starting and running tablets
            TAutoPtr<TEvLocal::TEvSyncTablets> eventSyncTablets = new TEvLocal::TEvSyncTablets();
            for (const auto& pr : InbootTablets) {
                auto* tablet = eventSyncTablets->Record.AddInbootTablets();
                tablet->SetTabletId(pr.first.first);
                tablet->SetFollowerId(pr.first.second);
                tablet->SetGeneration(pr.second.Generation);
                tablet->SetBootMode(pr.second.BootMode);

                ctx.Send(pr.second.Tablet, new TEvTablet::TEvUpdateConfig(ResourceProfiles));
            }
            for (const auto& pr : OnlineTablets) {
                auto* tablet = eventSyncTablets->Record.AddOnlineTablets();
                tablet->SetTabletId(pr.first.first);
                tablet->SetFollowerId(pr.first.second);
                tablet->SetGeneration(pr.second.Generation);
                tablet->SetBootMode(pr.second.BootMode);

                ctx.Send(pr.second.Tablet, new TEvTablet::TEvUpdateConfig(ResourceProfiles));
            }
            NTabletPipe::SendData(ctx, HivePipeClient, eventSyncTablets.Release());

            Connected = true;
            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar TEvPing - CONNECTED");
        }

        HiveGeneration = hiveGen;
        BootQueue = sender;

        // we send status of the 'local' to become online and available for new tablets
        SendStatusOk(ctx);

        ScheduleSendTabletMetrics(ctx);
    }

    void Handle(TEvLocal::TEvBootTablet::TPtr &ev, const TActorContext &ctx) {
        NKikimrLocal::TEvBootTablet &record = ev->Get()->Record;
        TIntrusivePtr<TTabletStorageInfo> info(TabletStorageInfoFromProto(record.GetInfo()));
        info->HiveId = HiveId;
        TTabletId tabletId(info->TabletID, record.GetFollowerId());
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvBootTablet tabletId:"
                    << tabletId
                    << (record.HasBootMode() && record.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_FOLLOWER ? ".Follower" : ".Leader")
                    << " storage:"
                    << info->ToString());
        Y_ABORT_UNLESS(!info->Channels.empty() && !info->Channels[0].History.empty());
        auto tabletType = info->TabletType;
        Y_ABORT_UNLESS(tabletType != TTabletTypes::TypeInvalid);
        ui32 suggestedGen = record.GetSuggestedGeneration();

        if (ev->Sender != BootQueue) {
            LOG_NOTICE(ctx, NKikimrServices::LOCAL,
                "TLocalNodeRegistrar: Handle TEvLocal::TEvBootTablet unexpected sender:%s",
                ev->Sender.ToString().c_str());
            ctx.Send(ev->Sender, new TEvLocal::TEvTabletStatus(
                TEvLocal::TEvTabletStatus::StatusBootQueueUnknown, tabletId, suggestedGen));
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvLocal::TEvBootTablet tabletType:"
                    << tabletType << " tabletId:" << tabletId << " suggestedGen:" << suggestedGen);

        TMap<TTabletTypes::EType, TLocalConfig::TTabletClassInfo>::const_iterator it = Config->TabletClassInfo.find(tabletType);
        if (it == Config->TabletClassInfo.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::LOCAL,
                       "TLocalNodeRegistrar: boot-tablet unknown tablet type: "
                       << tabletType << " for tablet: " << tabletId);
            ctx.Send(ev->Sender, new TEvLocal::TEvTabletStatus(
                TEvLocal::TEvTabletStatus::StatusTypeUnknown, tabletId, suggestedGen));
            return;
        }

        {
            auto it = OnlineTablets.find(tabletId);
            if (it != OnlineTablets.end()) {
                if (it->second.BootMode == NKikimrLocal::EBootMode::BOOT_MODE_FOLLOWER
                        && record.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_LEADER) {
                    // promote to leader
                    it->second.BootMode = NKikimrLocal::EBootMode::BOOT_MODE_LEADER;
                    it->second.Generation = suggestedGen;
                    tabletId.second = 0; // FollowerId = 0
                    TTabletEntry &entry = InbootTablets[tabletId];
                    entry = it->second;
                    entry.From = ctx.Now();
                    entry.BootMode = NKikimrLocal::EBootMode::BOOT_MODE_LEADER;
                    entry.Generation = suggestedGen;
                    ctx.Send(entry.Tablet, new TEvTablet::TEvPromoteToLeader(suggestedGen, info));
                    MarkDeadTablet(it->first, 0, TEvLocal::TEvTabletStatus::StatusSupersededByLeader, TEvTablet::TEvTabletDead::ReasonError, ctx);
                    OnlineTablets.erase(it);
                    LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                        "TLocalNodeRegistrar::Handle TEvLocal::TEvBootTablet follower tablet " << tabletId << " promoted to leader");
                    return;
                }
                ctx.Send(it->second.Tablet, new TEvTablet::TEvTabletStop(tabletId.first, TEvTablet::TEvTabletStop::ReasonStop));
                OnlineTablets.erase(it);
            }
        }

        if (record.GetBootMode() == NKikimrLocal::EBootMode::BOOT_MODE_LEADER && tabletId.second != 0) {
            LOG_WARN_S(ctx, NKikimrServices::LOCAL,
                       "TLocalNodeRegistrar: unsuccessful attempt to promote follower "
                       << "tabletId: " << tabletId << " suggestedGen: " << suggestedGen);
            tabletId.second = 0; // we change tabletid to leader because it was unsuccessful attempt to boot leader, not a follower
            ctx.Send(ev->Sender, new TEvLocal::TEvTabletStatus(TEvLocal::TEvTabletStatus::StatusFailed, tabletId, suggestedGen));
            return;
        }

        TTabletEntry &entry = InbootTablets[tabletId];
        if (entry.Tablet && entry.Generation != suggestedGen) {
            ctx.Send(entry.Tablet, new TEvents::TEvPoisonPill());
        }

        TTabletSetupInfo *setupInfo = it->second.SetupInfo.Get();
        switch (record.GetBootMode()) {
        case NKikimrLocal::BOOT_MODE_LEADER:
            entry.Tablet = setupInfo->Tablet(info.Get(), ctx.SelfID, ctx, suggestedGen, ResourceProfiles, TxCacheQuota);
            CounterStartAttempts->Inc();
            break;
        case NKikimrLocal::BOOT_MODE_FOLLOWER:
            entry.Tablet = setupInfo->Follower(info.Get(), ctx.SelfID, ctx, tabletId.second, ResourceProfiles, TxCacheQuota);
            CounterFollowerAttempts->Inc();
            break;
        }

        entry.Generation = suggestedGen;
        entry.From = ctx.Now();
        entry.TabletType = tabletType;
        entry.BootMode = record.GetBootMode();

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Handle TEvLocal::TEvBootTablet tabletId:" << tabletId << " tablet entry created");

        if (record.GetBootMode() == NKikimrLocal::BOOT_MODE_FOLLOWER) {
            MarkRunningTablet(tabletId, suggestedGen, ctx);
        }
    }

    void Handle(TEvLocal::TEvStopTablet::TPtr &ev, const TActorContext &ctx) {
        const NKikimrLocal::TEvStopTablet &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasTabletId());
        TTabletId tabletId(record.GetTabletId(), record.GetFollowerId());
        ui32 generation(record.GetGeneration());
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvStopTablet TabletId:" << tabletId << " Generation:" << generation);

        auto onlineTabletIt = OnlineTablets.find(tabletId);
        if (onlineTabletIt != OnlineTablets.end()) {
            if (generation == 0 || onlineTabletIt->second.Generation <= generation) {
                ctx.Send(onlineTabletIt->second.Tablet, new TEvTablet::TEvTabletStop(tabletId.first, TEvTablet::TEvTabletStop::ReasonStop));
            }
        } else {
            auto inbootTabletIt = InbootTablets.find(tabletId);
            if (inbootTabletIt != InbootTablets.end()) {
                if (generation == 0 || inbootTabletIt->second.Generation <= generation) {
                    ctx.Send(inbootTabletIt->second.Tablet, new TEvTablet::TEvTabletStop(tabletId.first, TEvTablet::TEvTabletStop::ReasonStop));
                }
            }
        }
    }

    void Handle(TEvLocal::TEvDeadTabletAck::TPtr &ev, const TActorContext &ctx) {
        const NKikimrLocal::TEvDeadTabletAck &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasTabletId());
        TTabletId tabletId(record.GetTabletId(), record.GetFollowerId());
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvDeadTabletAck TabletId:" << tabletId);
    }

    void Handle(TEvLocal::TEvTabletMetrics::TPtr& ev, const TActorContext& ctx) {
        TEvLocal::TEvTabletMetrics* msg = ev->Get();
        const TTabletId tabletId(msg->TabletId, msg->FollowerId);
        auto it = OnlineTablets.find(tabletId);
        if (it != OnlineTablets.end()) {
            const auto& metrics(msg->ResourceValues);
            auto before = it->second.ResourceValues.ByteSize();
            if (metrics.HasCPU()) {
                it->second.ResourceValues.SetCPU(metrics.GetCPU());
            }
            if (metrics.HasMemory()) {
                it->second.ResourceValues.SetMemory(metrics.GetMemory());
            }
            if (metrics.HasNetwork()) {
                it->second.ResourceValues.SetNetwork(metrics.GetNetwork());
            }
            if (metrics.HasStorage()) {
                it->second.ResourceValues.SetStorage(metrics.GetStorage());
            }
            if (metrics.GroupReadThroughputSize() > 0) {
                it->second.ResourceValues.ClearGroupReadThroughput();
                for (const auto& v : metrics.GetGroupReadThroughput()) {
                    it->second.ResourceValues.AddGroupReadThroughput()->CopyFrom(v);
                }
            }
            if (metrics.GroupWriteThroughputSize() > 0) {
                it->second.ResourceValues.ClearGroupWriteThroughput();
                for (const auto& v : metrics.GetGroupWriteThroughput()) {
                    it->second.ResourceValues.AddGroupWriteThroughput()->CopyFrom(v);
                }
            }
            if (metrics.GroupReadIopsSize() > 0) {
                it->second.ResourceValues.ClearGroupReadIops();
                for (const auto& v : metrics.GetGroupReadIops()) {
                    it->second.ResourceValues.AddGroupReadIops()->CopyFrom(v);
                }
            }
            if (metrics.GroupWriteIopsSize() > 0) {
                it->second.ResourceValues.ClearGroupWriteIops();
                for (const auto& v : metrics.GetGroupWriteIops()) {
                    it->second.ResourceValues.AddGroupWriteIops()->CopyFrom(v);
                }
            }
            auto after = it->second.ResourceValues.ByteSize();
            if (after == 0 && before == 0) {
                return;
            }
            auto uit = UpdatedTabletMetrics.find(tabletId);
            if (uit == UpdatedTabletMetrics.end()) {
                UpdatedTabletMetrics.emplace(tabletId, 1);
            } else {
                uit->second = 2;
            }
            if (Connected) {
                ScheduleSendTabletMetrics(ctx);
            }
        }
    }

    void Handle(TEvPrivate::TEvSendTabletMetrics::TPtr&, const TActorContext& ctx) {
        if (Connected) {
            TAutoPtr<TEvHive::TEvTabletMetrics> event = new TEvHive::TEvTabletMetrics;
            NKikimrHive::TEvTabletMetrics& record = event->Record;
            for (const auto& prTabletId : UpdatedTabletMetrics) {
                AddTabletMetrics(prTabletId.first, record);
            }
            if (UserPoolUsage != 0) {
                record.MutableTotalResourceUsage()->SetCPU(UserPoolUsage);
            }
            if (MemUsage != 0) {
                record.MutableTotalResourceUsage()->SetMemory(MemUsage);
            }
            record.SetTotalNodeUsage(NodeUsage);
            FillResourceMaximum(record.MutableResourceMaximum());
            NTabletPipe::SendData(ctx, HivePipeClient, event.Release());
            SendTabletMetricsTime = ctx.Now();
        } else {
            SendTabletMetricsInProgress = false;
        }
    }

    void ScheduleSendTabletMetrics(const TActorContext& ctx) {
        if (!SendTabletMetricsInProgress) {
            TDuration schedulePeriod = LastSendTabletMetricsDuration * 2 + TABLET_METRICS_BATCH_INTERVAL;
            schedulePeriod = Max(schedulePeriod, TABLET_METRICS_BATCH_INTERVAL);
            schedulePeriod = Min(schedulePeriod, TDuration::Seconds(60));
            ctx.Schedule(schedulePeriod, new TEvPrivate::TEvSendTabletMetrics());
            SendTabletMetricsInProgress = true;
        }
    }

    void AddTabletMetrics(TTabletId tabletId, NKikimrHive::TEvTabletMetrics& record) {
        auto it = OnlineTablets.find(tabletId);
        if (it != OnlineTablets.end()) {
            TOnlineTabletEntry& tablet = it->second;
            auto& metrics = *record.AddTabletMetrics();
            metrics.SetTabletID(tabletId.first);
            metrics.SetFollowerID(tabletId.second);
            metrics.MutableResourceUsage()->MergeFrom(tablet.ResourceValues);
        }
    }

    void Handle(TEvLocal::TEvTabletMetricsAck::TPtr& ev, const TActorContext& ctx) {
        TEvLocal::TEvTabletMetricsAck* msg = ev->Get();
        auto size = msg->Record.TabletIdSize();
        Y_ABORT_UNLESS(msg->Record.FollowerIdSize() == size);
        for (decltype(size) i = 0; i < size; ++i) {
            TTabletId tabletId(msg->Record.GetTabletId(i), msg->Record.GetFollowerId(i));
            auto uit = UpdatedTabletMetrics.find(tabletId);
            if (uit != UpdatedTabletMetrics.end() && --uit->second == 0) {
                UpdatedTabletMetrics.erase(uit);
                auto it = OnlineTablets.find(tabletId);
                if (it != OnlineTablets.end()) {
                    TOnlineTabletEntry& tablet = it->second;
                    tablet.ResourceValues.Clear();
                }
            }
        }
        LastSendTabletMetricsDuration = ctx.Now() - SendTabletMetricsTime;
        SendTabletMetricsInProgress = false;
        if (!UpdatedTabletMetrics.empty()) {
            ScheduleSendTabletMetrics(ctx);
        }
    }

    void Handle(TEvPrivate::TEvUpdateSystemUsage::TPtr&, const TActorContext&) {
        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()), new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest());
        Schedule(UPDATE_SYSTEM_USAGE_INTERVAL, new TEvPrivate::TEvUpdateSystemUsage());
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev, const TActorContext& ctx) {
        const NKikimrWhiteboard::TEvSystemStateResponse& record = ev->Get()->Record;
        if (!record.GetSystemStateInfo().empty()) {
            const NKikimrWhiteboard::TSystemStateInfo& info = record.GetSystemStateInfo(0);
            if (static_cast<ui32>(info.PoolStatsSize()) > AppData()->UserPoolId) {
                const auto& poolStats(info.GetPoolStats(AppData()->UserPoolId));
                CpuLimit = poolStats.limit() * 1'000'000; // microseconds
                UserPoolUsage = poolStats.usage() * CpuLimit; // microseconds
            }

            // Note: we use allocated memory because MemoryUsed(AnonRSS) has lag
            if (info.HasMemoryUsedInAlloc()) {
                MemUsage = info.GetMemoryUsedInAlloc();
            }

            double usage = 0;

            if (info.HasMemoryLimit()) {
                if (MemLimit != info.GetMemoryLimit()) {
                    MemLimit = info.GetMemoryLimit();
                    LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar MemoryLimit changed");
                    if (Connected) {
                        SendStatusOk(ctx);
                    }
                }
                usage = static_cast<double>(MemUsage) / MemLimit;
            }

            TVector<TString> poolsToMonitorForUsage = SplitString(AppData()->HiveConfig.GetPoolsToMonitorForUsage(), ",");

            for (const auto& poolInfo : info.poolstats()) {
                if (Find(poolsToMonitorForUsage, poolInfo.GetName()) != poolsToMonitorForUsage.end()) {
                    usage = std::max(usage, poolInfo.usage());
                }
            }

            NodeUsage = usage;

            ScheduleSendTabletMetrics(ctx);
        }
    }

    void Handle(TEvLocal::TEvAlterTenant::TPtr &ev, const TActorContext &ctx) {
        auto &info = ev->Get()->TenantInfo;
        ResourceLimit = info.ResourceLimit;

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "Updated resoure limit: "
                    << ResourceLimit.ShortDebugString());

        UpdateCacheQuota();

        SendStatusOk(ctx);
    }

    void MarkRunningTablet(TTabletId tabletId, ui32 generation, const TActorContext& ctx) {
        auto inbootIt = InbootTablets.find(tabletId);
        if (inbootIt == InbootTablets.end())
            return;
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: tablet "
                    << tabletId
                    << " marked as running at generation "
                    << generation);
        NTabletPipe::SendData(ctx, HivePipeClient, new TEvLocal::TEvTabletStatus(TEvLocal::TEvTabletStatus::StatusOk, tabletId, generation));
        OnlineTablets.emplace(tabletId, inbootIt->second);
        InbootTablets.erase(inbootIt);
    }

    void Handle(TEvTablet::TEvRestored::TPtr &ev, const TActorContext&) {
        TEvTablet::TEvRestored *msg = ev->Get();

        if (msg->Follower) // ignore follower notifications
            return;

        CounterRestored->Inc(); // always update counter for every tablet, even out-of-date ones. it's about tracking not resource allocation
    }

    void Handle(TEvTablet::TEvReady::TPtr &ev, const TActorContext &ctx) {
        TEvTablet::TEvReady *msg = ev->Get();

        const auto tabletId = msg->TabletID;
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvTablet::TEvReady tablet "
                    << tabletId
                    << " generation "
                    << msg->Generation);
        auto inbootIt = std::find_if(InbootTablets.begin(), InbootTablets.end(), [&](const auto& pr) -> bool {
            return pr.second.Tablet == ev->Sender;
        });
        if (inbootIt == InbootTablets.end())
            return;
        TTabletEntry &entry = inbootIt->second;
        if (msg->Generation < entry.Generation) {
            LOG_WARN_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvTablet::TEvReady tablet "
                       << tabletId
                       << " ready with generation "
                       << msg->Generation
                       << " but we are waiting for generation "
                       << entry.Generation
                       << " - ignored");
            return;
        }
        MarkRunningTablet(inbootIt->first, msg->Generation, ctx);
    }

    void Handle(TEvTablet::TEvCutTabletHistory::TPtr &ev, const TActorContext &ctx) {
        if (Connected) // must be 'connected' check
            NTabletPipe::SendData(ctx, HivePipeClient, ev.Get()->Release().Release());
    }

    void Handle(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
        TEvTablet::TEvTabletDead *msg = ev->Get();
        const auto tabletId = msg->TabletID;
        const ui32 generation = msg->Generation;
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Handle TEvTablet::TEvTabletDead tabletId:"
                    << tabletId << " generation:" << generation << " reason:" << (ui32)msg->Reason);

        switch (msg->Reason) {
        case TEvTablet::TEvTabletDead::ReasonBootLocked:
            CounterCancelLocked->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSSTimeout:
            CounterCancelSSTimeout->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBootRace:
            CounterCancelRace->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBootBSError:
            CounterCancelBootBSError->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSuggestOutdated:
            CounterCancelOutdated->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBootSSError:
            CounterCancelBootSSError->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonPill:
            CounterCancelPoisonPill->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonDemotedByStateStorage:
            CounterCancelDemotedBySS->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonBSError:
            CounterCancelBSError->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonInconsistentCommit:
            CounterCancelInconsistentCommit->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonIsolated:
            CounterCancelIsolated->Inc();
            break;
        case TEvTablet::TEvTabletDead::ReasonDemotedByBlobStorage:
            CounterCancelDemotedByBS->Inc();
            break;
        default:
            CounterCancelUnknownReason->Inc();
            break;
        }

        // known tablets should be in online-tablets or in inboot-tablets (with correct generation).
        auto onlineIt = std::find_if(OnlineTablets.begin(), OnlineTablets.end(), [&](const auto& pr) -> bool {
            return pr.second.Tablet == ev->Sender;
        });
        if (onlineIt != OnlineTablets.end()) { // from online list
            MarkDeadTablet(onlineIt->first, generation, TEvLocal::TEvTabletStatus::StatusFailed, msg->Reason, ctx);
            OnlineTablets.erase(onlineIt);
            UpdateEstimate();
            return;
        }

        auto inbootIt = std::find_if(InbootTablets.begin(), InbootTablets.end(), [&](const auto& pr) -> bool {
            return pr.second.Tablet == ev->Sender;
        });
        if (inbootIt != InbootTablets.end() && inbootIt->second.Generation <= generation) {
            MarkDeadTablet(inbootIt->first, generation, TEvLocal::TEvTabletStatus::StatusBootFailed, msg->Reason, ctx);
            InbootTablets.erase(inbootIt);
            return;
        }

        // deprecated tablet, we don't care
    }

    void HandlePoison(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar: HandlePoison");
        Die(ctx);
    }

    void HandleDrainNodeResult(TEvHive::TEvDrainNodeResult::TPtr &ev, const TActorContext &ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::LOCAL, "Drain node result received. Online tablets: " << OnlineTablets.size());
        Y_ABORT_UNLESS(SentDrainNode);
        Y_UNUSED(ev);
        UpdateEstimate();
        if (!DrainResultReceived) {
            DrainResultReceived = true;
            LastDrainRequest->OnReceive();
        }
    }

    void UpdateEstimate() {
        if (SentDrainNode) {
            i32 diff = static_cast<i32>(OnlineTablets.size()) - PrevEstimate;
            PrevEstimate = OnlineTablets.size();
            LastDrainRequest->UpdateEstimate(diff);
        }
    }

    void HandleDrainTimeout(TEvPrivate::TEvLocalDrainTimeout::TPtr &ev, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::LOCAL, "Drain node result received: timeout. Online tablets: " << OnlineTablets.size());
        Y_ABORT_UNLESS(SentDrainNode);
        Y_UNUSED(ev);
        UpdateEstimate();
        if (!DrainResultReceived) {
            DrainResultReceived = true;
            LastDrainRequest->OnReceive();
        }
    }

    void HandleDrain(TEvLocal::TEvLocalDrainNode::TPtr &ev, const TActorContext &ctx) {
        if (!HivePipeClient) {
            TryToRegister(ctx);
        }

        if (!SentDrainNode) {
            LOG_NOTICE_S(ctx, NKikimrServices::LOCAL, "Send drain node to hive: " << HiveId << ". Online tablets: " << OnlineTablets.size());
            SentDrainNode = true;
            ev->Get()->DrainProgress->OnSend();
            LastDrainRequest = ev->Get()->DrainProgress;
            UpdateEstimate();
            NTabletPipe::SendData(ctx, HivePipeClient, new TEvHive::TEvDrainNode(SelfId().NodeId()));
            ctx.Schedule(DRAIN_NODE_TIMEOUT, new TEvPrivate::TEvLocalDrainTimeout());
            ev->Get()->DrainProgress->OnReceive();
        } else {
            ev->Get()->DrainProgress->OnReceive();
        }
    }

    void UpdateCacheQuota() {
        ui64 mem = ResourceLimit.GetMemory();
        if (mem)
            TxCacheQuota->ChangeQuota(mem / 4);
        else
            TxCacheQuota->ChangeQuota(Max<i64>());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::LOCAL_ACTOR;
    }

    TLocalNodeRegistrar(const TActorId &owner, ui64 hiveId, TVector<TSubDomainKey> servicedDomains,
                        const NKikimrTabletBase::TMetrics &resourceLimit, TIntrusivePtr<TLocalConfig> config,
                        ::NMonitoring::TDynamicCounterPtr counters)
        : Owner(owner)
        , HiveId(hiveId)
        , ServicedDomains(std::move(servicedDomains))
        , Connected(false)
        , Config(config)
        , HiveGeneration(0)
        , SendTabletMetricsInProgress(false)
        , ResourceLimit(resourceLimit)
        , Counters(counters)
    {
        Y_ABORT_UNLESS(!ServicedDomains.empty());
        TxCacheQuota = new TSharedQuota(Counters->GetCounter("UsedTxDataCache"),
                                        Counters->GetCounter("TxDataCacheSize"));

        CounterStartAttempts = Counters->GetCounter("Local_StartAttempts", true);
        CounterFollowerAttempts = Counters->GetCounter("Local_FollowerAttempts", true);
        CounterRestored = Counters->GetCounter("Local_Restored", true);
        CounterCancelLocked = Counters->GetCounter("Local_CancelLocked", true);
        CounterCancelSSTimeout = Counters->GetCounter("Local_CancelSSTimeout", true);
        CounterCancelRace = Counters->GetCounter("Local_CancelRace", true);
        CounterCancelBootBSError = Counters->GetCounter("Local_CancelBootBSError", true);
        CounterCancelOutdated = Counters->GetCounter("Local_CancelOutdated", true);
        CounterCancelBootSSError = Counters->GetCounter("Local_CancelBootSSError", true);
        CounterCancelPoisonPill = Counters->GetCounter("Local_CancelPoisonPill", true);
        CounterCancelDemotedBySS = Counters->GetCounter("Local_CancelDemotedBySS", true);
        CounterCancelBSError = Counters->GetCounter("Local_CancelBSError", true);
        CounterCancelInconsistentCommit = Counters->GetCounter("Local_CancelInconsistentCommit", true);
        CounterCancelIsolated = Counters->GetCounter("Local_CancelIsolated", true);
        CounterCancelDemotedByBS = Counters->GetCounter("Local_CancelDemotedByBS", true);
        CounterCancelUnknownReason = Counters->GetCounter("Local_CancelUnknownReason", true);

        UpdateCacheQuota();
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocalNodeRegistrar::Bootstrap");
        Send(SelfId(), new TEvPrivate::TEvUpdateSystemUsage());
        StartTime = ctx.Now();
        TryToRegister(ctx);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvRestored, Handle); // tablet restored, update counter
            HFunc(TEvTablet::TEvReady, Handle); // tablet ready, notify hive about update
            HFunc(TEvTablet::TEvTabletDead, Handle); // tablet dead, notify queue about update
            HFunc(TEvTablet::TEvCutTabletHistory, Handle);
            HFunc(TEvLocal::TEvBootTablet, Handle); // command to boot tablet
            HFunc(TEvLocal::TEvPing, Handle); // command to update link to per-local boot-queue
            HFunc(TEvLocal::TEvStopTablet, Handle); // stop tablet
            HFunc(TEvLocal::TEvDeadTabletAck, Handle);
            HFunc(TEvLocal::TEvEnumerateTablets, Handle);
            HFunc(TEvLocal::TEvTabletMetrics, Handle);
            HFunc(TEvLocal::TEvTabletMetricsAck, Handle);
            HFunc(TEvLocal::TEvAlterTenant, Handle);
            HFunc(TEvLocal::TEvReconnect, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvPrivate::TEvSendTabletMetrics, Handle);
            HFunc(TEvPrivate::TEvUpdateSystemUsage, Handle);
            HFunc(TEvPrivate::TEvLocalDrainTimeout, HandleDrainTimeout);
            HFunc(TEvLocal::TEvLocalDrainNode, HandleDrain);
            HFunc(TEvHive::TEvDrainNodeResult, HandleDrainNodeResult);
            HFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            default:
                LOG_DEBUG(*TlsActivationContext, NKikimrServices::LOCAL, "TLocalNodeRegistrar: Unhandled in StateWork type: %" PRIx32
                    " event: %s", ev->GetTypeRewrite(), ev->ToString().data());
                break;
        }
    }

};

class TDomainLocal : public TActorBootstrapped<TDomainLocal> {
    struct TResolveTask {
        TRegistrationInfo Info;
        TVector<TActorId> Senders;
    };

    struct TTenantInfo {
        TTenantInfo(const TRegistrationInfo &info, const TSubDomainKey &domainKey)
            : Info(info)
            , DomainKey(domainKey)
        {}

        TTenantInfo(const TTenantInfo &other) = default;
        TTenantInfo(TTenantInfo &&other) = default;

        TRegistrationInfo Info;
        TVector<TActorId> Locals;
        TActorId Subscriber;
        TVector<TTabletId> HiveIds;
        THashMap<TString, TString> Attributes;
        TSubDomainKey DomainKey;
    };

    TString Domain;
    ui64 SchemeRoot;
    TVector<ui64> HiveIds;
    TActorId SchemeShardPipe;
    NTabletPipe::TClientConfig PipeConfig;
    THashMap<TString, TResolveTask> ResolveTasks;
    TIntrusivePtr<TLocalConfig> Config;
    THashMap<TString, TTenantInfo> RunningTenants;
    TString LogPrefix;

    void OpenPipe(const TActorContext &ctx)
    {
        ClosePipe(ctx);
        SchemeShardPipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, SchemeRoot, PipeConfig));
    }

    void ClosePipe(const TActorContext &ctx)
    {
        if (SchemeShardPipe) {
            NTabletPipe::CloseClient(ctx, SchemeShardPipe);
            SchemeShardPipe = TActorId();
        }
    }

    void SendResolveRequest(const TRegistrationInfo &info, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                    LogPrefix << "Send resolve request for " << info.TenantName
                    << " to schemeshard " << SchemeRoot);

        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(info.TenantName);
        NTabletPipe::SendData(ctx.SelfID, SchemeShardPipe, request.Release());
    }

    void SendStatus(const TString &tenant,
                    TActorId recipient,
                    const TActorContext &ctx)
    {
        TAutoPtr<TEvLocal::TEvTenantStatus> ev;

        auto it = RunningTenants.find(tenant);
        if (it != RunningTenants.end())
            ev = new TEvLocal::TEvTenantStatus(tenant,
                                               TEvLocal::TEvTenantStatus::STARTED,
                                               it->second.Info.ResourceLimit,
                                               it->second.Attributes,
                                               it->second.DomainKey);

        else
            ev = new TEvLocal::TEvTenantStatus(tenant,
                                               TEvLocal::TEvTenantStatus::STOPPED);

        ctx.Send(recipient, ev.Release());
    }

    void SendStatus(const TString &tenant,
                    TEvLocal::TEvTenantStatus::EStatus status,
                    const TString &msg,
                    TActorId recipient,
                    const TActorContext &ctx)
    {
        TAutoPtr<TEvLocal::TEvTenantStatus> ev = new TEvLocal::TEvTenantStatus(tenant, status, msg);
        ctx.Send(recipient, ev.Release());
    }

    void SendStatus(const TString &tenant,
                    const TVector<TActorId> &recipients,
                    const TActorContext &ctx)
    {
        for (auto &r : recipients)
            SendStatus(tenant, r, ctx);
    }

    void SendStatus(const TString &tenant,
                    TEvLocal::TEvTenantStatus::EStatus status,
                    const TString &msg,
                    const TVector<TActorId> &recipients,
                    const TActorContext &ctx)
    {
        for (auto &r : recipients)
            SendStatus(tenant, status, msg, r, ctx);
    }

    void RegisterLocalNode(const TString &tenant,
                           const NKikimrTabletBase::TMetrics &resourceLimit,
                           ui64 hiveId,
                           const TVector<TSubDomainKey> &servicedDomains,
                           const TActorContext &ctx)
    {
        auto counters = GetServiceCounters(AppData(ctx)->Counters, "tablets");
        auto actor = new TLocalNodeRegistrar(SelfId(), hiveId, servicedDomains, resourceLimit, Config, counters);
        TActorId actorId = ctx.Register(actor);

        RunningTenants.at(tenant).Locals.push_back(actorId);

        TActorId localRegistrarServiceId = MakeLocalRegistrarID(ctx.SelfID.NodeId(), hiveId);
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(localRegistrarServiceId, actorId);
    }

    void RegisterAsDomain(const TRegistrationInfo &info,
                          const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(!RunningTenants.contains(info.TenantName));
        const auto domainKey = TSubDomainKey(SchemeRoot, 1);
        RunningTenants.emplace(std::make_pair(info.TenantName, TTenantInfo(info, domainKey)));
        for (auto id : HiveIds) {
            RegisterLocalNode(info.TenantName, info.ResourceLimit, id, {domainKey}, ctx);

            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                        LogPrefix << "Binding to hive " << id <<
                        " at domain " << info.DomainName <<
                        " (allocated resources: " << info.ResourceLimit.ShortDebugString() << ")");
        }
    }

    void RegisterAsSubDomain(const NKikimrScheme::TEvDescribeSchemeResult &rec,
                             const TResolveTask &task,
                             const TVector<TTabletId> hiveIds,
                             const TActorContext &ctx)
    {
        const auto &domainDesc = rec.GetPathDescription().GetDomainDescription();
        const auto domainKey = TSubDomainKey(domainDesc.GetDomainKey());

        Y_ABORT_UNLESS(!RunningTenants.contains(task.Info.TenantName));
        TTenantInfo info(task.Info, domainKey);
        for (auto &attr : rec.GetPathDescription().GetUserAttributes())
            info.Attributes.emplace(std::make_pair(attr.GetKey(), attr.GetValue()));
        RunningTenants.emplace(std::make_pair(task.Info.TenantName, info));
        const TActorId whiteboardServiceId(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()));
        Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateSetTenant(task.Info.TenantName));
        for (TTabletId hId : hiveIds) {
            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                        LogPrefix << "Binding tenant " << task.Info.TenantName
                        << " to hive " << hId
                        << " (allocated resources: " << task.Info.ResourceLimit.ShortDebugString() << ")");
            RegisterLocalNode(task.Info.TenantName, task.Info.ResourceLimit, hId, {domainKey}, ctx);
        }
    }

    void HandlePoison(const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, LogPrefix << "HandlePoison");

        for (auto &pr : RunningTenants) {
            for (auto aid : pr.second.Locals) {
                LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                            LogPrefix << "Send poison pill to local of " << pr.second.Info.TenantName);
                ctx.Send(aid, new TEvents::TEvPoisonPill);
            }
            if (pr.second.Subscriber) {
                LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                            LogPrefix << "Send poison pill to scheme subscriber of " << pr.second.Info.TenantName);
                ctx.Send(pr.second.Subscriber, new TEvents::TEvPoisonPill);
                pr.second.Subscriber = TActorId();
            }
            ctx.Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId()),
                     new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRemoveTenant(pr.first));
        }

        ClosePipe(ctx);
        Die(ctx);
    }

    void HandlePipe(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext &ctx)
    {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                    LogPrefix << "TDomainLocal::TEvClientConnected for " << Domain
                    << " shard " << msg->TabletId);

        if (msg->Status != NKikimrProto::EReplyStatus::OK) {
            OpenPipe(ctx);
            return;
        }

        for (auto &pr : ResolveTasks)
            SendResolveRequest(pr.second.Info, ctx);
    }

    void HandlePipe(TEvTabletPipe::TEvClientDestroyed::TPtr ev, const TActorContext &ctx)
    {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                    LogPrefix << "TEvTabletPipe::TEvClientDestroyed from tablet " << msg->TabletId);

        OpenPipe(ctx);
    }

    void HandleResolve(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev, const TActorContext &ctx)
    {
        const NKikimrScheme::TEvDescribeSchemeResult &rec = ev->Get()->GetRecord();

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                    LogPrefix << "HandleResolve from schemeshard " << SchemeRoot
                    << ": " << rec.ShortDebugString());

        if (!ResolveTasks.contains(rec.GetPath())) {
            LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                        LogPrefix << "Missing task for " << rec.GetPath());
            return;
        }

        const TResolveTask &task = ResolveTasks.find(rec.GetPath())->second;

        using EDescStatus = NKikimrScheme::EStatus;
        if (rec.GetStatus() != EDescStatus::StatusSuccess) {
            LOG_ERROR_S(ctx, NKikimrServices::LOCAL,
                        LogPrefix << " Receive TEvDescribeSchemeResult with bad status "
                        << NKikimrScheme::EStatus_Name(rec.GetStatus()) <<
                        " reason is <" << rec.GetReason() << ">" <<
                        " while resolving subdomain " << task.Info.DomainName);

            SendStatus(rec.GetPath(), TEvLocal::TEvTenantStatus::UNKNOWN_TENANT,
                       rec.GetReason(), task.Senders, ctx);
            ResolveTasks.erase(rec.GetPath());
            return;
        }

        Y_ABORT_UNLESS(rec.HasPathDescription());
        Y_ABORT_UNLESS(rec.GetPathDescription().HasSelf());
        if (rec.GetPathDescription().GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeSubDomain
            && rec.GetPathDescription().GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeExtSubDomain) {
            LOG_CRIT_S(ctx, NKikimrServices::LOCAL,
                       LogPrefix << " Resolve subdomain fail, requested path "
                       << task.Info.DomainName << " has invalid path type "
                       << NKikimrSchemeOp::EPathType_Name(rec.GetPathDescription().GetSelf().GetPathType()));

            SendStatus(rec.GetPath(), TEvLocal::TEvTenantStatus::UNKNOWN_TENANT,
                       rec.GetPath() + " is not a tenant path", task.Senders, ctx);
            ResolveTasks.erase(rec.GetPath());
            return;
        }
        Y_ABORT_UNLESS(rec.GetPathDescription().HasDomainDescription());
        const auto &domainDesc = rec.GetPathDescription().GetDomainDescription();
        Y_ABORT_UNLESS(domainDesc.GetDomainKey().GetSchemeShard() == SchemeRoot);

        TVector<TTabletId> hiveIds(HiveIds);
        TTabletId hiveId = domainDesc.GetProcessingParams().GetHive();
        if (hiveId) {
            hiveIds.emplace_back(hiveId);
        }
        TTabletId sharedHiveId = domainDesc.GetSharedHive();
        if (sharedHiveId) {
            hiveIds.emplace_back(sharedHiveId);
        }
        RegisterAsSubDomain(rec, task, hiveIds, ctx);

        const TString &path = rec.GetPath();
        auto itTenant = RunningTenants.find(path);
        if (itTenant != RunningTenants.end()) {
            TTenantInfo& tenant = itTenant->second;

            tenant.HiveIds = hiveIds;

            SendStatus(path, task.Senders, ctx);
            ResolveTasks.erase(path);

            // subscribe for schema updates
            THolder<IActor> subscriber(CreateSchemeBoardSubscriber(SelfId(), path, ESchemeBoardSubscriberDeletionPolicy::Majority));
            tenant.Subscriber = Register(subscriber.Release());
        } else {
            LOG_WARN_S(ctx, NKikimrServices::LOCAL,
                       LogPrefix << " Local tenant info not found, requested path " << task.Info.DomainName);
        }
    }

    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyUpdate::TPtr &ev, const TActorContext &ctx)
    {
        TString path = ev->Get()->DescribeSchemeResult.GetPath();
        auto itTenant = RunningTenants.find(path);
        if (itTenant != RunningTenants.end()) {
            TTenantInfo& tenant = itTenant->second;
            TTabletId hiveId = ev->Get()->DescribeSchemeResult.GetPathDescription().GetDomainDescription().GetProcessingParams().GetHive();
            if (hiveId) {
                auto itHiveId = Find(tenant.HiveIds, hiveId);
                if (itHiveId == tenant.HiveIds.end()) {
                    const auto &domainDesc = ev->Get()->DescribeSchemeResult.GetPathDescription().GetDomainDescription();
                    TVector<TSubDomainKey> servicedDomains = {TSubDomainKey(domainDesc.GetDomainKey())};

                    LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                                LogPrefix << "Binding tenant " << tenant.Info.TenantName
                                << " to hive " << hiveId
                                << " (allocated resources: " << tenant.Info.ResourceLimit.ShortDebugString() << ")");
                    RegisterLocalNode(tenant.Info.TenantName, tenant.Info.ResourceLimit, hiveId, servicedDomains, ctx);
                    tenant.HiveIds.emplace_back(hiveId);
                }
            }
        }
    }

    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyDelete::TPtr &ev, const TActorContext &ctx)
    {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandleTenant(TEvLocal::TEvAddTenant::TPtr &ev, const TActorContext &ctx)
    {
        auto &info = ev->Get()->TenantInfo;

        // Already started?
        if (RunningTenants.contains(info.TenantName)) {
            SendStatus(info.TenantName, ev->Sender, ctx);
            return;
        }

        // In-fly?
        auto it = ResolveTasks.find(info.TenantName);
        if (it != ResolveTasks.end()) {
            it->second.Senders.push_back(ev->Sender);
            return;
        }

        if (info.TenantIsDomain()) {
            RegisterAsDomain(info, ctx);
            SendStatus(info.TenantName, ev->Sender, ctx);
        } else {
            ResolveTasks.emplace(info.TenantName, TResolveTask{info, {ev->Sender}});
            if (SchemeShardPipe)
                SendResolveRequest(info, ctx);
            else
                OpenPipe(ctx);
        }
    }

    void HandleTenant(TEvLocal::TEvRemoveTenant::TPtr &ev, const TActorContext &ctx)
    {
        auto &tenant = ev->Get()->TenantName;

        // In-fly?
        auto it = ResolveTasks.find(tenant);
        if (it != ResolveTasks.end()) {
            Y_ABORT_UNLESS(!RunningTenants.contains(tenant));
            SendStatus(tenant, it->second.Senders, ctx);
            ResolveTasks.erase(it);
        } else {
            auto it = RunningTenants.find(tenant);
            if (it != RunningTenants.end()) {
                for (auto aid : it->second.Locals)
                    ctx.Send(aid, new TEvents::TEvPoisonPill());
                if (it->second.Subscriber) {
                    ctx.Send(it->second.Subscriber, new TEvents::TEvPoisonPill());
                    it->second.Subscriber = TActorId();
                }
                ctx.Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId()),
                         new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRemoveTenant(it->first));
                RunningTenants.erase(it);
            }
        }
        SendStatus(ev->Get()->TenantName, ev->Sender, ctx);
    }

    void HandleDrain(TEvLocal::TEvLocalDrainNode::TPtr &ev, const TActorContext& ctx) {
        for(auto& hiveId: HiveIds) {
            LOG_NOTICE_S(ctx, NKikimrServices::LOCAL, "Forward drain node to local, hive id: " << hiveId);
            ev->Get()->DrainProgress->OnSend();
            TActorId localRegistrarServiceId = MakeLocalRegistrarID(ctx.SelfID.NodeId(), hiveId);
            ctx.Send(localRegistrarServiceId, new TEvLocal::TEvLocalDrainNode(ev->Get()->DrainProgress));
        }
        ev->Get()->DrainProgress->OnReceive();
    }

    void HandleTenant(TEvLocal::TEvAlterTenant::TPtr &ev, const TActorContext &ctx)
    {
        auto &info = ev->Get()->TenantInfo;

        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL,
                    LogPrefix << "Alter tenant " << info.TenantName);

        auto it = RunningTenants.find(info.TenantName);
        if (it != RunningTenants.end()) {
            it->second.Info = info;
            for (auto &aid : it->second.Locals)
                ctx.Send(aid, new TEvLocal::TEvAlterTenant(info));
        }

        SendStatus(info.TenantName, ev->Sender, ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::LOCAL_ACTOR;
    }

    TDomainLocal(const TDomainsInfo &domainsInfo, const TDomainsInfo::TDomain &domain,
                 TIntrusivePtr<TLocalConfig> config)
        : Domain(domain.Name)
        , SchemeRoot(domain.SchemeRoot)
        , Config(config)
    {
        if (const ui64 tabletId = domainsInfo.GetHive(); tabletId != TDomainsInfo::BadTabletId) {
            HiveIds.push_back(tabletId);
        }

        PipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        LogPrefix = Sprintf("TDomainLocal(%s): ", Domain.data());
    }

    void Bootstrap(const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::LOCAL, LogPrefix << "Bootstrap");

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);

            HFunc(TEvTabletPipe::TEvClientConnected, HandlePipe);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipe);

            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleResolve);

            HFunc(TEvLocal::TEvAddTenant, HandleTenant);
            HFunc(TEvLocal::TEvRemoveTenant, HandleTenant);
            HFunc(TEvLocal::TEvAlterTenant, HandleTenant);

            HFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleSchemeBoard);
            HFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleSchemeBoard);

            HFunc(TEvLocal::TEvLocalDrainNode, HandleDrain);
        default:
            Y_ABORT("Unexpected event for TDomainLocal");
            break;
        }
    }
};

class TLocal : public TActorBootstrapped<TLocal> {
    TIntrusivePtr<TLocalConfig> Config;
    THashMap<TString, TActorId> DomainLocals;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::LOCAL_ACTOR;
    }

    TLocal(TLocalConfig *cfg)
        : Config(cfg)
    {}

    void HandlePoison(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocal: HandlePoison");
        for (auto &pr : DomainLocals)
            ctx.Send(pr.second, new TEvents::TEvPoisonPill());
        Die(ctx);
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::LOCAL, "TLocal::Bootstrap");
        Become(&TThis::StateWork);
    }

    bool ForwardToDomainLocal(TAutoPtr<IEventHandle> ev, const TString &domainName, const TActorContext &ctx)
    {
        auto &domainsInfo = *AppData(ctx)->DomainsInfo;
        auto *domain = domainsInfo.GetDomainByName(domainName);
        if (!domain) {
            LOG_ERROR_S(ctx, NKikimrServices::LOCAL, "Unknown domain " << domainName);
            return false;
        }

        if (!DomainLocals.contains(domainName)) {
            auto actor = new TDomainLocal(domainsInfo, *domain, Config);
            DomainLocals[domainName] = ctx.Register(actor);
        }
        ctx.Forward(ev, DomainLocals[domainName]);
        return true;
    }

    void SendUnknownDomain(const TString &tenant, TActorId sender, const TActorContext &ctx)
    {
        TAutoPtr<TEvLocal::TEvTenantStatus> ev
            = new TEvLocal::TEvTenantStatus(tenant, TEvLocal::TEvTenantStatus::UNKNOWN_TENANT,
                                            "Tenant is in unknown domain");
        ctx.Send(sender, ev.Release());
    }

    void ForwardOrReplyError(TAutoPtr<IEventHandle> ev, const TRegistrationInfo &info, const TActorContext &ctx)
    {
        auto sender = ev->Sender;

        if (!ForwardToDomainLocal(std::move(ev), info.DomainName, ctx))
            SendUnknownDomain(info.TenantName, sender, ctx);
    }

    void HandleTenant(TEvLocal::TEvAddTenant::TPtr &ev, const TActorContext &ctx)
    {
        TRegistrationInfo info = ev->Get()->TenantInfo;
        ForwardOrReplyError(ev.Release(), info, ctx);
    }

    void HandleTenant(TEvLocal::TEvRemoveTenant::TPtr &ev, const TActorContext &ctx)
    {
        TRegistrationInfo info(ev->Get()->TenantName);
        ForwardOrReplyError(ev.Release(), info, ctx);
    }

    void HandleDrain(TEvLocal::TEvLocalDrainNode::TPtr &ev, const TActorContext& ctx) {
        for(auto& x: DomainLocals) {
            ev->Get()->DrainProgress->OnSend();
            ctx.Send(x.second, new TEvLocal::TEvLocalDrainNode(ev->Get()->DrainProgress));
        }
        ev->Get()->DrainProgress->OnReceive();
    }

    void HandleTenant(TEvLocal::TEvAlterTenant::TPtr &ev, const TActorContext &ctx)
    {
        TRegistrationInfo info = ev->Get()->TenantInfo;
        ForwardOrReplyError(ev.Release(), info, ctx);
    }

    void HandleTenant(TEvLocal::TEvTenantStatus::TPtr &ev, const TActorContext &/*ctx*/)
    {
        Y_ABORT_UNLESS(ev->Get()->Status != TEvLocal::TEvTenantStatus::UNKNOWN_TENANT,
                 "Unknown tenant %s", ev->Get()->TenantName.data());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);

            HFunc(TEvLocal::TEvAddTenant, HandleTenant);
            HFunc(TEvLocal::TEvRemoveTenant, HandleTenant);
            HFunc(TEvLocal::TEvAlterTenant, HandleTenant);
            HFunc(TEvLocal::TEvTenantStatus, HandleTenant);
            HFunc(TEvLocal::TEvLocalDrainNode, HandleDrain);

        default:
            ALOG_DEBUG(NKikimrServices::LOCAL,
                        "TLocal: Unhandled in StateResolveSubDomain type: " <<  ev->GetTypeRewrite() <<
                        " event: " << ev->ToString());
            break;
        }
    }
};

IActor* CreateLocal(TLocalConfig *config) {
    return new TLocal(config);
}

}
