#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "hive_impl.h"
#include "hive_log.h"
#include "node_info.h"

namespace NKikimr {
namespace NHive {

class THiveFill : public NActors::TActorBootstrapped<THiveFill>, public ISubActor {
protected:
    constexpr static ui64 TIMEOUT = 1800000; // 30 minutes
    THive* Hive;
    TVector<TFullTabletId> Tablets;
    TVector<TFullTabletId>::iterator NextKick;
    ui32 KickInFlight;
    ui32 Movements;
    TNodeId NodeId;
    TActorId Initiator;

    TString GetLogPrefix() const {
        return Hive->GetLogPrefix();
    }

    void PassAway() override {
        BLOG_I("Fill " << SelfId() << " finished with " << Movements << " movements made");
        Hive->RemoveSubActor(this);
        Hive->BalancerNodes.erase(NodeId);
        return IActor::PassAway();
    }

    void Cleanup() override {
        PassAway();
    }

    TString GetDescription() const override {
        return TStringBuilder() << "Fill(" << NodeId << ")";
    }

    TSubActorId GetId() const override {
        return SelfId().LocalId();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TActorContext& ctx) {
        ctx.Send(Initiator, new TEvHive::TEvFillNodeResult(status));
        Die(ctx);
    }

    bool CanKickNextTablet() const {
        return NextKick != Tablets.end() && KickInFlight < Hive->GetDrainInflight();
    }

    void KickNextTablet(const TActorContext& ctx) {
        while (CanKickNextTablet()) {
            TFullTabletId tabletId = *NextKick;
            TTabletInfo* tablet = Hive->FindTablet(tabletId);
            if (tablet != nullptr && tablet->IsAlive() && tablet->NodeId != NodeId) {
                THive::TBestNodeResult result = Hive->FindBestNode(*tablet);
                if (std::holds_alternative<TNodeInfo*>(result)) {
                    TNodeInfo* node = std::get<TNodeInfo*>(result);
                    if (node->Id == NodeId) {
                        tablet->ActorsToNotifyOnRestart.emplace_back(SelfId()); // volatile settings, will not persist upon restart
                        ++KickInFlight;
                        ++Movements;
                        BLOG_D("Fill " << SelfId() << " moving tablet " << tablet->ToString() << " " << tablet->GetResourceValues()
                               << " from node " << tablet->Node->Id << " " << tablet->Node->ResourceValues
                               << " to node " << node->Id << " " << node->ResourceValues);
                        Hive->TabletCounters->Cumulative()[NHive::COUNTER_FILL_EXECUTED].Increment(1);
                        Hive->RecordTabletMove(THive::TTabletMoveInfo(TInstant::Now(), *tablet, tablet->Node->Id, node->Id));
                        Hive->Execute(Hive->CreateRestartTablet(tablet->GetFullTabletId(), node->Id), ctx);
                    }
                }
            }
            ++NextKick;
        }
        if (KickInFlight == 0) {
            return ReplyAndDie(NKikimrProto::OK, ctx);
        }
    }

    void Handle(TEvPrivate::TEvRestartComplete::TPtr& ev, const TActorContext& ctx) {
        BLOG_D("Fill " << SelfId() << " received " << ev->Get()->Status << " for tablet " << ev->Get()->TabletId);
        --KickInFlight;
        KickNextTablet(ctx);
    }

    void Timeout(const TActorContext& ctx) {
        ReplyAndDie(NKikimrProto::TIMEOUT, ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HIVE_BALANCER_ACTOR;
    }

    THiveFill(THive* hive, TNodeId nodeId, const TActorId& initiator)
        : Hive(hive)
        , NextKick(Tablets.end())
        , KickInFlight(0)
        , Movements(0)
        , NodeId(nodeId)
        , Initiator(initiator)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TNodeInfo* nodeInfo = Hive->FindNode(NodeId);
        if (nodeInfo != nullptr) {
            TVector<TTabletInfo*> tablets;
            tablets.reserve(Hive->Tablets.size());
            Tablets.reserve(Hive->Tablets.size());
            for (auto it = Hive->Tablets.begin(); it != Hive->Tablets.end(); ++it) {
                it->second.UpdateWeight();
                tablets.push_back(&it->second);
                for (auto& follower : it->second.Followers) {
                    follower.UpdateWeight();
                    tablets.push_back(&follower);
                }
            }
            Sort(tablets, [](const TTabletInfo* a, const TTabletInfo* b) -> bool { return a->Weight > b->Weight; });
            for (TTabletInfo* tabletInfo : tablets) {
                Tablets.push_back(tabletInfo->GetFullTabletId());
            }
            NextKick = Tablets.begin();
            Become(&THiveFill::StateWork, ctx, TDuration::MilliSeconds(TIMEOUT), new TEvents::TEvWakeup());
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "Fill " << SelfId() << " started for node " << NodeId);
            KickNextTablet(ctx);
        } else {
            ReplyAndDie(NKikimrProto::ERROR, ctx);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::PoisonPill, Die);
            HFunc(TEvPrivate::TEvRestartComplete, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }
};

void THive::StartHiveFill(TNodeId nodeId, const TActorId& initiator) {
    if (BalancerNodes.emplace(nodeId).second) {
        auto* balancer = new THiveFill(this, nodeId, initiator);
        SubActors.emplace_back(balancer);
        RegisterWithSameMailbox(balancer);
    } else {
        BLOG_W("It's not possible to start fill on node " << nodeId << ", the node is already busy");
        Send(initiator, new TEvHive::TEvFillNodeResult(NKikimrProto::ALREADY));
    }
}

} // NHive
} // NKikimr

