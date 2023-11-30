#include "tablet.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>

namespace NKikimr {

class TTabletStatusCheckRequest : public TActorBootstrapped<TTabletStatusCheckRequest> {
private:
    const TActorId ReplyTo;
    TIntrusiveConstPtr<TTabletStorageInfo> Info;
    ui32 RequestsLeft;
    TVector<ui32> LightYellowMoveGroups;
    TVector<ui32> YellowStopGroups;

    void Handle(TEvBlobStorage::TEvStatusResult::TPtr &ev, const TActorContext &ctx) {
        const TEvBlobStorage::TEvStatusResult *msg = ev->Get();
        --RequestsLeft;

        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            LightYellowMoveGroups.push_back(ev->Cookie);
        }
        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            YellowStopGroups.push_back(ev->Cookie);
        }

        if (RequestsLeft == 0) {
            ctx.Send(ReplyTo, new TEvTablet::TEvCheckBlobstorageStatusResult(std::move(LightYellowMoveGroups),
                        std::move(YellowStopGroups)));
            return Die(ctx);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_ACTOR;
    }

    TTabletStatusCheckRequest(const TActorId &replyTo, TTabletStorageInfo *info)
        : ReplyTo(replyTo)
        , Info(info)
        , RequestsLeft(0)
    {}

    void Bootstrap(const TActorContext &ctx) {
        THashSet<ui32> seen;
        for (const auto &channel : Info->Channels) {
            const ui32 groupToCheck = channel.History.back().GroupID;
            if (seen.insert(groupToCheck).second) {
                SendToBSProxy(ctx, groupToCheck, new TEvBlobStorage::TEvStatus(TInstant::Max()), groupToCheck);
                ++RequestsLeft;
            }
        }
        Become(&TTabletStatusCheckRequest::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvStatusResult, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }
};

IActor* CreateTabletDSChecker(const TActorId &replyTo, TTabletStorageInfo *info) {
    return new TTabletStatusCheckRequest(replyTo, info);
}

}
