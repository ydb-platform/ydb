#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "hive_impl.h"
#include "hive_log.h"
#include "node_info.h"

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
    TNodeId NodeId;
    bool DownBefore = false;
    TActorId DomainHivePipeClient;
    TTabletId DomainHiveId = 0;
    ui32 DomainMovements = 0;
    TDrainSettings Settings;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        if (DomainHivePipeClient) {
            NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
        }
        Hive->RemoveSubActor(this);
        Hive->BalancerNodes.erase(NodeId);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TString GetDescription() const override {
        return TStringBuilder() << "Drain(" << NodeId << ")";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        BLOG_I("Drain " << SelfId() << " finished with " << Movements << " movements made");
        TNodeInfo* nodeInfo = Hive->FindNode(NodeId);
        if (nodeInfo != nullptr) {
            if (!DownBefore) {
                nodeInfo->SetDown(false);
            }
        }
        Hive->Execute(Hive->CreateSwitchDrainOff(NodeId, std::move(Settings), status, Movements + DomainMovements));
        PassAway();
    }

    bool CanKickNextTablet() const {
        ui32 inFlight = Settings.DrainInFlight ? Settings.DrainInFlight : Hive->GetDrainInflight();
        return NextKick != Tablets.end() && KickInFlight < inFlight;
    }

    void KickNextTablet() {
        while (CanKickNextTablet()) {
            TFullTabletId tabletId = *NextKick;
            TTabletInfo* tablet = Hive->FindTablet(tabletId);
            if (tablet != nullptr && tablet->IsAlive() && tablet->NodeId == NodeId) {
                THive::TBestNodeResult result = Hive->FindBestNode(*tablet);
                if (std::holds_alternative<TNodeInfo*>(result)) {
                    TNodeInfo* node = std::get<TNodeInfo*>(result);
                    tablet->ActorsToNotifyOnRestart.emplace_back(SelfId()); // volatile settings, will not persist upon restart
                    ++KickInFlight;
                    ++Movements;
                    BLOG_D("Drain " << SelfId() << " moving tablet "
                                << tablet->ToString() << " " << tablet->GetResourceValues()
                                << " from node " << tablet->Node->Id << " " << tablet->Node->ResourceValues
                                << " to node " << node->Id << " " << node->ResourceValues);
                    Hive->TabletCounters->Cumulative()[NHive::COUNTER_DRAIN_EXECUTED].Increment(1);
                    Hive->RecordTabletMove(THive::TTabletMoveInfo(TInstant::Now(), *tablet, tablet->Node->Id, node->Id));
                    Hive->Execute(Hive->CreateRestartTablet(tabletId, node->Id));
                } else {
                    if (std::holds_alternative<THive::TNoNodeFound>(result)) {
                        Hive->TabletCounters->Cumulative()[NHive::COUNTER_DRAIN_FAILED].Increment(1);
                        BLOG_D("Drain " << SelfId() << " could not move tablet " << tablet->ToString() << " " << tablet->GetResourceValues()
                               << " from node " << tablet->Node->Id << " " << tablet->Node->ResourceValues);
                    } else if (std::holds_alternative<THive::TTooManyTabletsStarting>(result)){
                        BLOG_D("Drain " << SelfId() << " could not move tablet " << tablet->ToString() << " and will try again later");
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
            NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
            DomainHivePipeClient = {};
        }
        KickNextTablet();
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev) {
        BLOG_D("Drain " << SelfId() << " received " << ev->Get()->Status << " for tablet " << ev->Get()->TabletId);
        --KickInFlight;
        KickNextTablet();
    }

    void Handle(TEvHive::TEvDrainNodeResult::TPtr& ev) {
        BLOG_D("Drain " << SelfId() << " received status from domain hive " << ev->Get()->Record.ShortDebugString());
        BLOG_I("Drain " << SelfId() << " continued for node " << NodeId << " with " << Tablets.size() << " tablets");
        DomainDrainCompleted(ev->Get()->Record.GetMovements());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK && DomainHiveId != 0) {
            BLOG_W("Drain " << SelfId() << " pipe to hive " << DomainHiveId << " failed to connect");
            DomainDrainCompleted();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == DomainHivePipeClient) {
            BLOG_W("Drain " << SelfId() << " pipe to hive " << DomainHiveId << " destroyed - retrying");
            if (DomainHivePipeClient) {
                NTabletPipe::CloseClient(SelfId(), DomainHivePipeClient);
            }
            RequestDrainFromDomainHive();
        }
    }

    void Timeout() {
        ReplyAndDie(NKikimrProto::TIMEOUT);
    }

    void RequestDrainFromDomainHive() {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 13};
        DomainHivePipeClient = Register(NTabletPipe::CreateClient(SelfId(), DomainHiveId, pipeConfig));
        THolder<TEvHive::TEvDrainNode> event = MakeHolder<TEvHive::TEvDrainNode>(NodeId);
        event->Record.SetKeepDown(Settings.KeepDown);
        event->Record.SetPersist(Settings.Persist);
        event->Record.SetDrainInFlight(Settings.DrainInFlight);
        NTabletPipe::SendData(SelfId(), DomainHivePipeClient, event.Release());
        BLOG_I("Drain " << SelfId() << " forwarded for node " << NodeId << " to hive " << DomainHiveId);
    }


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_BALANCER_ACTOR;
    }

    THiveDrain(THive* hive, TNodeId nodeId, TDrainSettings settings)
        : Hive(hive)
        , NextKick(Tablets.end())
        , KickInFlight(0)
        , Movements(0)
        , NodeId(nodeId)
        , Settings(std::move(settings))
    {}

    void Bootstrap() {
        TNodeInfo* nodeInfo = Hive->FindNode(NodeId);
        if (nodeInfo != nullptr) {
            {
                Tablets.reserve(nodeInfo->GetTabletsRunning());
                for (const auto& [object, tablets] : nodeInfo->TabletsOfObject) {
                    for (TTabletInfo* tabletInfo : tablets) {
                        if (tabletInfo->GetVolatileState() == TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING) {
                            Tablets.push_back(tabletInfo->GetFullTabletId());
                        }
                    }
                }
            }
            NextKick = Tablets.begin();
            DownBefore = nodeInfo->Down;
            if (!DownBefore) {
                nodeInfo->SetDown(true);
            }

            if (nodeInfo->ServicedDomains.size() == 1) {
                TDomainInfo* domainInfo = Hive->FindDomain(nodeInfo->ServicedDomains.front());
                if (domainInfo != nullptr) {
                    if (domainInfo->HiveId != 0 && domainInfo->HiveId != Hive->TabletID()) {
                        DomainHiveId = domainInfo->HiveId;
                        RequestDrainFromDomainHive();
                        Become(&THiveDrain::StateWork, TDuration::MilliSeconds(TIMEOUT), new TEvents::TEvWakeup());
                        return;
                    }
                }
            }

            Become(&THiveDrain::StateWork, TDuration::MilliSeconds(TIMEOUT), new TEvents::TEvWakeup());
            BLOG_I("Drain " << SelfId() << " started for node " << NodeId << " with " << Tablets.size() << " tablets");
            KickNextTablet();
        } else {
            ReplyAndDie(NKikimrProto::ERROR);
        }
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
};

void THive::StartHiveDrain(TNodeId nodeId, TDrainSettings settings) {
    if (BalancerNodes.emplace(nodeId).second) {
        auto* balancer = new THiveDrain(this, nodeId, std::move(settings));
        SubActors.emplace_back(balancer);
        RegisterWithSameMailbox(balancer);
    } else {
        BLOG_W("It's not possible to start drain on node " << nodeId << ", the node is already busy");
    }
}

} // NHive
} // NKikimr
