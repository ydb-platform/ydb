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
    const std::unordered_set<TNodeId> Nodes; // nodes of the target at the start of the drain
    const std::unordered_set<TNodeId> ReservedNodes; // the ones the drain has added to BalancerNodes
    std::unordered_map<TNodeId, bool> DownBefore;
    TActorId DomainHivePipeClient;
    TTabletId DomainHiveId = 0;
    ui32 DomainMovements = 0;
    ui64 SeqNo = 0;
    TDrainSettings Settings;
    bool Skipped = false;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        if (DomainHivePipeClient) {
            Hive->Requests.FinishRequests(DomainHivePipeClient);
            NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
        }
        Hive->RemoveSubActor(this);
        // the nodes are released as they were reserved, as the target pile may have changed or even gone meanwhile
        for (const TNodeId nodeId : ReservedNodes) {
            Hive->BalancerNodes.erase(nodeId);
        }
        for (const TNodeId nodeId : Nodes) {
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
            TNodeInfo* nodeInfo = Hive->FindNode(nodeId);
            if (nodeInfo != nullptr) {
                if (!downBefore) {
                    nodeInfo->SetDown(false, EHiveEventReason::DrainFinished);
                }
            }
        }
        auto* switchDrainOff = Hive->CreateSwitchDrainOff(Target, std::move(Settings), status, Movements + DomainMovements, Skipped);
        // a read-only tx completes synchronously, and its Complete may restart this drain
        PassAway();
        Hive->Execute(switchDrainOff);
    }

    bool IsTargetTablet(const TTabletInfo& tablet) const {
        if (std::holds_alternative<TBridgePileId>(Target)) {
            // a STARTING tablet has Node set but NodeId 0; a node drain owns its node
            return tablet.Node != nullptr && Nodes.contains(tablet.Node->Id) && !tablet.Node->Drain;
        }
        return Nodes.contains(tablet.NodeId); // node drains keep NodeId: STARTING stays with its booter
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
                        if (const auto* pileId = std::get_if<TBridgePileId>(&Target); pileId && !tablet->GetNodeFilter().IsAllowedPile(*pileId)) {
                            Skipped = true; // the tablet must leave the pile, so the flag stays for a later resume
                        }
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

    THiveDrain(THive* hive, TDrainTarget target, TDrainSettings settings, std::unordered_set<TNodeId> nodes,
            std::unordered_set<TNodeId> reservedNodes, bool skipped)
        : Hive(hive)
        , NextKick(Tablets.end())
        , KickInFlight(0)
        , Movements(0)
        , Target(target)
        , Nodes(std::move(nodes))
        , ReservedNodes(std::move(reservedNodes))
        , Settings(std::move(settings))
        , Skipped(skipped)
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
                if (auto it = nodeInfo->Tablets.find(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN);
                        it != nodeInfo->Tablets.end() && std::holds_alternative<TBridgePileId>(Target)) {
                    // not resynced yet: keep the flag, the node's TTxStatus after resync resumes the drain;
                    // locked tablets stay Unknown under LockedTabletsSendMetrics
                    Skipped |= std::any_of(it->second.begin(), it->second.end(), [](const TTabletInfo* tablet) {
                        return !(tablet->IsLeader() && tablet->AsLeader().IsLockedToActor());
                    });
                }
            } else {
                return ReplyAndDie(NKikimrProto::ERROR);
            }
        }
        for (const TNodeId nodeId : ReservedNodes) {
            if (TNodeInfo* nodeInfo = Hive->FindNode(nodeId)) {
                DownBefore[nodeId] = nodeInfo->Down;
                nodeInfo->SetDown(true, EHiveEventReason::DrainStarted);
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
    TGetNodes getNodes{this};
    std::unordered_set<TNodeId> nodes = std::visit(getNodes, target);
    std::unordered_set<TNodeId> reservedNodes;
    for (const TNodeId nodeId : nodes) {
        if (BalancerNodes.emplace(nodeId).second) {
            reservedNodes.insert(nodeId);
        }
    }
    if (!reservedNodes.empty()) {
        auto* balancer = new THiveDrain(this, target, std::move(settings), std::move(nodes), std::move(reservedNodes), getNodes.SkippedNodeDrain);
        SubActors.emplace_back(balancer);
        RegisterWithSameMailbox(balancer);
        return balancer;
    } else {
        YDB_LOG_WARN("Cannot start drain: target is already busy",
            {"logPrefix", GetLogPrefix()},
            {"target", target});
        return nullptr;
    }
}

void THive::StartPileDrain(TBridgePileInfo& pile) {
    if (!pile.Drain || IsPrimaryPile(pile.Id)) {
        return; // a primary pile hosts the drained tablets, so its flag is stale
    }
    if (pile.DrainActor) {
        pile.DrainRetry = true;
        return;
    }
    if (std::none_of(Nodes.begin(), Nodes.end(), [this](const auto& item) { return item.second.IsAlive() && IsPrimaryPile(item.second.BridgePileId); })) {
        return; // nowhere to move tablets yet; a primary pile node connect resumes the drain
    }
    pile.DrainActor = StartHiveDrain(pile.Id, TDrainSettings{.DownPolicy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN, .Forward = false});
    if (pile.DrainActor) {
        ++pile.DrainEpoch;
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
