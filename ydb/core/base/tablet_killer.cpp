#include "statestorage.h"
#include "tablet.h"
#include "tabletid.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {

class TTabletKillRequest : public TActorBootstrapped<TTabletKillRequest> {
private:
    const ui64 TabletId;
    const ui32 NodeId;
    const ui32 MaxGeneration;

    void Handle(TEvStateStorage::TEvInfo::TPtr &ev, const TActorContext &ctx) {
        TEvStateStorage::TEvInfo *msg = ev->Get();
        TActorId LeaderActor;
        if (NodeId == 0) {
            LeaderActor = msg->CurrentLeader;
        } else {
            if (msg->CurrentLeader && msg->CurrentLeader.NodeId() == NodeId) {
                LeaderActor = msg->CurrentLeader;
            } else {
                for (const auto& pr : msg->Followers) {
                    if (pr.first.NodeId() == NodeId) {
                        LeaderActor = pr.first;
                        break;
                    }
                }
            }
        }
        if (LeaderActor && msg->CurrentGeneration <= MaxGeneration)
            ctx.Send(LeaderActor, new TEvents::TEvPoisonPill());
        return Die(ctx);
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_KILLER;
    }

    TTabletKillRequest(ui64 tabletId, ui32 nodeId, ui32 maxGeneration)
        : TabletId(tabletId)
        , NodeId(nodeId)
        , MaxGeneration(maxGeneration)
    {}

    void Bootstrap(const TActorContext &ctx) {
        const TActorId stateStorageProxyId = MakeStateStorageProxyID();
        ctx.Send(stateStorageProxyId, new TEvStateStorage::TEvLookup(TabletId, 0));
        Become(&TTabletKillRequest::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvStateStorage::TEvInfo, Handle);
        }
    }
};

IActor* CreateTabletKiller(ui64 tabletId, ui32 nodeId, ui32 maxGeneration) {
    return new TTabletKillRequest(tabletId, nodeId, maxGeneration);
}

}

