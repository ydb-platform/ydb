#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "drain.h"
#include "hive_impl.h"
#include "hive_log.h"
#include "node_info.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class THiveDrain : public NActors::TActorBootstrapped<THiveDrain>, public ISubActor {
protected:
    constexpr static ui64 TIMEOUT = 1800000; // 30 minutes
    THive* Hive;
    TVector<TFullTabletId> Tablets;
    TVector<TFullTabletId>::iterator NextKick;
    ui32 KickInFlight;
    ui32 Movements;
    TDrainTarget Target;
    const std::unordered_set<TNodeId> Nodes;
    std::unordered_map<TNodeId, bool> DownBefore;
    TActorId DomainHivePipeClient;
    TTabletId DomainHiveId = 0;
    ui32 DomainMovements = 0;
    ui64 SeqNo = 0;
    TDrainSettings Settings;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        if (DomainHivePipeClient) {
            Hive->Requests.FinishRequests(DomainHivePipeClient);
            NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
        }
        Hive->RemoveSubActor(this);
        for (const TNodeId nodeId : Nodes) {
            Hive->BalancerNodes.erase(nodeId);
            if (auto node = Hive->FindNode(nodeId); node && node->DrainActor == this) {
                node->DrainActor = nullptr;
            }
        }
        if (const auto* pileId = std::get_if<TBridgePileId>(&Target)) {
            if (auto it = Hive->BridgePiles.find(*pileId); it != Hive->BridgePiles.end() && it->second.DrainActor == this) {
                it->second.DrainActor = nullptr;
            }
        }
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TString GetDescription() const override {
        return TStringBuilder() << "Drain(" << Target << ")";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        YDB_LOG_INFO("Drain finished",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"movements", Movements});
        for (auto [nodeId, downBefore] : DownBefore) {
            if (TNodeInfo* nodeInfo = Hive->FindNode(nodeId); nodeInfo != nullptr && !downBefore) {
                nodeInfo->SetDown(false, EHiveEventReason::DrainFinished);
            }
        }
        auto* switchDrainOff = Hive->CreateSwitchDrainOff(Target, status, Movements + DomainMovements);
        // Complete may run synchronously and start another drain.
        PassAway();
        Hive->Execute(switchDrainOff);
    }

    bool IsTargetTablet(const TTabletInfo& tablet) const {
        if (const auto* pileId = std::get_if<TBridgePileId>(&Target)) {
            return tablet.Node != nullptr && Nodes.contains(tablet.Node->Id) && !tablet.GetNodeFilter().IsAllowedPile(*pileId);
        }
        return Nodes.contains(tablet.NodeId);
    }

    bool CanKickNextTablet() const {
        ui32 inFlight = Settings.DrainInFlight ? Settings.DrainInFlight : Hive->GetDrainInflight();
        return NextKick != Tablets.end() && KickInFlight < inFlight;
    }

    void KickNextTablet() {
        while (CanKickNextTablet()) {
            TFullTabletId tabletId = *NextKick;
            TTabletInfo* tablet = Hive->FindTablet(tabletId);
            if (tablet != nullptr && tablet->IsAlive() && IsTargetTablet(*tablet)) {
                THive::TBestNodeResult result = Hive->FindBestNode(*tablet);
                if (std::holds_alternative<TNodeInfo*>(result)) {
                    TNodeInfo* node = std::get<TNodeInfo*>(result);
                    tablet->ActorsToNotifyOnRestart.emplace_back(SelfId()); // volatile settings, will not persist upon restart
                    ++KickInFlight;
                    ++Movements;
                    YDB_LOG_DEBUG("Drain moving tablet",
                        {"logPrefix", GetLogPrefix()},
                        {"selfId", SelfId()},
                        {"tablet", tablet->ToString()},
                        {"sourceNodeId", tablet->Node->Id},
                        {"targetNodeId", node->Id});
                    Hive->TabletCounters->Cumulative()[NHive::COUNTER_DRAIN_EXECUTED].Increment(1);
                    Hive->RecordTabletMove(THive::TTabletMoveInfo(TInstant::Now(), *tablet, tablet->Node->Id, node->Id), EHiveEventReason::Drain);
                    Hive->Execute(Hive->CreateRestartTablet(tabletId, node->Id));
                } else {
                    if (std::holds_alternative<THive::TNoNodeFound>(result) || std::holds_alternative<THive::TNotEnoughResources>(result)) {
                        Hive->TabletCounters->Cumulative()[NHive::COUNTER_DRAIN_FAILED].Increment(1);
                        YDB_LOG_DEBUG("Drain could not find target node for tablet",
                            {"logPrefix", GetLogPrefix()},
                            {"selfId", SelfId()},
                            {"tablet", tablet->ToString()},
                            {"sourceNodeId", tablet->Node->Id});
                    } else if (std::holds_alternative<THive::TTooManyTabletsStarting>(result)){
                        YDB_LOG_DEBUG("Drain postponed tablet move due to too many tablets starting",
                            {"logPrefix", GetLogPrefix()},
                            {"selfId", SelfId()},
                            {"tablet", tablet->ToString()});
                        Hive->WaitToMoveTablets(SelfId());
                        return;
                    }
                }
            }
            ++NextKick;
        }
        if (KickInFlight == 0) {
            return ReplyAndDie(NKikimrProto::OK);
        }
    }

    void DomainDrainCompleted(ui32 movements = 0) {
        Movements += movements;
        if (DomainHivePipeClient) {
            Hive->Requests.FinishRequests(DomainHivePipeClient);
            NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
            DomainHivePipeClient = {};
        }
        KickNextTablet();
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev) {
        YDB_LOG_DEBUG("Drain received TEvRestartComplete for tablet",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"status", ev->Get()->Status},
            {"tabletId", ev->Get()->TabletId});
        --KickInFlight;
        KickNextTablet();
    }

    void Handle(TEvHive::TEvDrainNodeResult::TPtr& ev) {
        YDB_LOG_DEBUG("Drain received status from domain hive",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"ev", ev->Get()->Record.ShortDebugString()});
        YDB_LOG_INFO("Drain continued after domain hive response",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"target", Target},
            {"tabletsCount", Tablets.size()});
        DomainDrainCompleted(ev->Get()->Record.GetMovements());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK && DomainHiveId != 0) {
            YDB_LOG_WARN("Drain pipe to hive failed to connect",
                {"logPrefix", GetLogPrefix()},
                {"selfId", SelfId()},
                {"domainHiveId", DomainHiveId});
            DomainDrainCompleted();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == DomainHivePipeClient) {
            YDB_LOG_WARN("Drain pipe to hive destroyed - retrying",
                {"logPrefix", GetLogPrefix()},
                {"selfId", SelfId()},
                {"domainHiveId", DomainHiveId});
            if (DomainHivePipeClient) {
                Hive->Requests.FinishRequests(DomainHivePipeClient);
                NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
            }
            Y_ABORT_UNLESS(std::holds_alternative<TNodeId>(Target));
            RequestDrainFromDomainHive(std::get<TNodeId>(Target));
        }
    }

    void Timeout() {
        ReplyAndDie(NKikimrProto::TIMEOUT);
    }

    void RequestDrainFromDomainHive(TNodeId nodeId) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 13};
        DomainHivePipeClient = Register(NTabletPipe::CreateClient(SelfId(), DomainHiveId, pipeConfig));
        THolder<TEvHive::TEvDrainNode> event = MakeHolder<TEvHive::TEvDrainNode>(nodeId);
        event->Record.SetDownPolicy(Settings.DownPolicy);
        event->Record.SetPersist(Settings.Persist);
        event->Record.SetDrainInFlight(Settings.DrainInFlight);
        event->Record.SetSeqNo(SeqNo);
        ui32 cookie = Hive->Requests.AddRequest(event.Get(), DomainHivePipeClient);
        NTabletPipe::SendData(SelfId(), DomainHivePipeClient, event.Release(), cookie);
        YDB_LOG_INFO("Drain forwarded for node to domain hive",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"nodeId", nodeId},
            {"domainHiveId", DomainHiveId});
    }


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_BALANCER_ACTOR;
    }

    THiveDrain(THive* hive, TDrainTarget target, TDrainSettings settings, std::unordered_set<TNodeId> nodes)
        : Hive(hive)
        , NextKick(Tablets.end())
        , KickInFlight(0)
        , Movements(0)
        , Target(target)
        , Nodes(std::move(nodes))
        , Settings(std::move(settings))
    {}

    void Bootstrap() {
        for (const TNodeId nodeId : Nodes) {
            TNodeInfo* nodeInfo = Hive->FindNode(nodeId);
            if (nodeInfo != nullptr) {
                Tablets.reserve(Tablets.size() + nodeInfo->GetTabletsRunning());
                for (const auto& [object, tablets] : nodeInfo->TabletsOfObject) {
                    for (TTabletInfo* tabletInfo : tablets) {
                        if (tabletInfo->CanBeAlive()) {
                            Tablets.push_back(tabletInfo->GetFullTabletId());
                        }
                    }
                }
                DownBefore[nodeId] = nodeInfo->Down;
                nodeInfo->SetDown(true, EHiveEventReason::DrainStarted);
            } else {
                return ReplyAndDie(NKikimrProto::ERROR);
            }
        }
        NextKick = Tablets.begin();
        if (Settings.Forward) {
            Y_ABORT_UNLESS(std::holds_alternative<TNodeId>(Target));
            TNodeId nodeId = std::get<TNodeId>(Target);
            TNodeInfo* nodeInfo = Hive->FindNode(nodeId);
            SeqNo = nodeInfo->DrainSeqNo;

            if (auto domainKey = nodeInfo->GetServicedDomain()) {
                TDomainInfo* domainInfo = Hive->FindDomain(domainKey);
                if (domainInfo != nullptr) {
                    if (domainInfo->HiveId != 0 && domainInfo->HiveId != Hive->TabletID()) {
                        DomainHiveId = domainInfo->HiveId;
                        RequestDrainFromDomainHive(nodeId);
                        Become(&THiveDrain::StateWork, TDuration::MilliSeconds(TIMEOUT), new TEvents::TEvWakeup());
                        return;
                    }
                }
            }
        }

        Become(&THiveDrain::StateWork, TDuration::MilliSeconds(TIMEOUT), new TEvents::TEvWakeup());
        YDB_LOG_INFO("Drain started",
            {"logPrefix", GetLogPrefix()},
            {"selfId", SelfId()},
            {"target", Target},
            {"tabletsCount", Tablets.size()});
        KickNextTablet();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            hFunc(TEvPrivate::TEvRestartComplete, Handle);
            hFunc(TEvHive::TEvDrainNodeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
            cFunc(TEvPrivate::EvCanMoveTablets, KickNextTablet);
        }
    }

    float GetProgress() {
        if (Tablets.empty()) {
            return 0.0;
        }
        return (float)Movements / Tablets.size();
    }
};

THiveDrain* THive::StartHiveDrain(TDrainTarget target, TDrainSettings settings) {
    std::unordered_set<TNodeId> nodes = std::visit(TGetNodes{this}, target);
    std::erase_if(nodes, [this](TNodeId nodeId) { return !BalancerNodes.emplace(nodeId).second; });
    if (nodes.empty()) {
        YDB_LOG_WARN("Cannot start drain: no available nodes",
            {"logPrefix", GetLogPrefix()},
            {"target", target});
        return nullptr;
    }
    auto* balancer = new THiveDrain(this, target, std::move(settings), std::move(nodes));
    SubActors.emplace_back(balancer);
    RegisterWithSameMailbox(balancer);
    return balancer;
}

void THive::StartPileDrain(TBridgePileInfo& pile) {
    if (!pile.Drain || pile.DrainActor || IsPrimaryPile(pile.Id)) {
        return;
    }
    if (std::none_of(Nodes.begin(), Nodes.end(), [this](const auto& item) {
        const auto& node = item.second;
        return node.IsAlive() && !node.Down && IsPrimaryPile(node.BridgePileId);
    })) {
        return;
    }
    pile.DrainActor = StartHiveDrain(pile.Id, TDrainSettings{.DownPolicy = NKikimrHive::DRAIN_POLICY_NO_DOWN, .Forward = false});
}

bool THive::HasTabletsToDrain(TBridgePileId pileId) const {
    if (!BridgeInfo || IsPrimaryPile(pileId)) {
        return false;
    }
    for (const auto& [nodeId, node] : Nodes) {
        if (node.BridgePileId != pileId) {
            continue;
        }
        for (const auto& [state, tablets] : node.Tablets) {
            for (const auto* tablet : tablets) {
                if (tablet->CanBeAlive() &&
                    !(tablet->IsLeader() && tablet->AsLeader().IsLockedToActor()) &&
                    !tablet->GetNodeFilter().IsAllowedPile(pileId)) {
                    return true;
                }
            }
        }
    }
    return false;
}

void THive::ProcessPileDrains() {
    if (ProcessPileDrainsScheduled || !BridgeInfo) {
        return;
    }
    ProcessPileDrainsScheduled = true;
    Send(SelfId(), new TEvPrivate::TEvProcessPileDrains());
}

void THive::Handle(TEvPrivate::TEvProcessPileDrains::TPtr&) {
    ProcessPileDrainsScheduled = false;
    if (!BridgeInfo || std::none_of(BridgePiles.begin(), BridgePiles.end(), [](const auto& item) { return item.second.Drain; })) {
        return;
    }
    for (auto& [pileId, pile] : BridgePiles) {
        if (!pile.Drain) {
            continue;
        }
        if (pile.DrainActor) {
            pile.DrainRetry = true;
            continue;
        }
        if (!HasTabletsToDrain(pileId)) {
            Execute(CreateSwitchDrainOff(pileId, NKikimrProto::OK, 0));
        } else {
            StartPileDrain(pile);
        }
    }
}

void THive::Handle(TEvHive::TEvRequestDrainInfo::TPtr& ev) {
    auto nodeId = ev->Get()->Record.GetNodeId();
    auto* node = FindNode(nodeId);
    auto tenantHive = GetPipeToTenantHive(node);
    if (tenantHive) {
        auto sender = ev->Sender;
        NTabletPipe::SendData(sender, *tenantHive, std::move(ev)->Get());
        return;
    }
    auto response = std::make_unique<TEvHive::TEvResponseDrainInfo>();
    if (node) {
        response->Record.SetNodeId(nodeId);
        response->Record.SetDrainSeqNo(node->DrainSeqNo);
        if (node->Drain) {
            response->Record.MutableDrainInProgress();
            if (node->DrainActor) {
                auto progress = node->DrainActor->GetProgress();
                response->Record.MutableDrainInProgress()->SetProgress(progress);
            }
        } else {
            response->Record.MutableNotRunning();
        }
    }

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

} // NHive
} // NKikimr
