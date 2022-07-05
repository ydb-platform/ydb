#include "tablet_impl.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <util/generic/set.h>

namespace NKikimr {

class TTabletReqBlockBlobStorageGroup : public TActorBootstrapped<TTabletReqBlockBlobStorageGroup> {
    const TActorId Owner;
    const ui64 TabletId;
    const ui32 GroupId;
    const ui32 Generation;

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason = { }) {
        Send(Owner, new TEvTabletBase::TEvBlockBlobStorageResult(status, TabletId, reason));
        PassAway();
    }

    void SendRequest() {
        const TActorId proxy = MakeBlobStorageProxyID(GroupId);
        THolder<TEvBlobStorage::TEvBlock> event(new TEvBlobStorage::TEvBlock(TabletId, Generation, TInstant::Max()));
        event->IsMonitored = false;
        SendToBSProxy(TlsActivationContext->AsActorContext(), proxy, event.Release());
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        return ReplyAndDie(NKikimrProto::ERROR, "BlobStorage proxy unavailable");
    }

    void Handle(TEvBlobStorage::TEvBlockResult::TPtr &ev) {
        const TEvBlobStorage::TEvBlockResult *msg = ev->Get();

        switch (msg->Status) {
        case NKikimrProto::OK:
            return ReplyAndDie(NKikimrProto::OK);
        case NKikimrProto::ALREADY:
        case NKikimrProto::BLOCKED:
        case NKikimrProto::RACE:
        case NKikimrProto::NO_GROUP:
            // The request will never succeed
            return ReplyAndDie(msg->Status, msg->ErrorReason);
        default:
            return ReplyAndDie(NKikimrProto::ERROR, msg->ErrorReason);
        }
    }

    STFUNC(StateWait) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvBlockResult, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_BLOCK_BS;
    }

    void Bootstrap() {
        SendRequest();
        Become(&TThis::StateWait);
    }

    TTabletReqBlockBlobStorageGroup(const TActorId &owner, ui64 tabletId, ui32 groupId, ui32 gen)
        : Owner(owner)
        , TabletId(tabletId)
        , GroupId(groupId)
        , Generation(gen)
    {}
};

class TTabletReqBlockBlobStorage : public TActorBootstrapped<TTabletReqBlockBlobStorage> {
    const TActorId Owner;
    TIntrusiveConstPtr<TTabletStorageInfo> Info;
    const ui32 Generation;
    const bool BlockPrevEntry;

    ui32 Replied = 0;
    TVector<TActorId> ReqActors;

    void PassAway() override {
        for (auto &x : ReqActors)
            if (x)
                Send(x, new TEvents::TEvPoisonPill());

        TActor::PassAway();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason = { }) {
        Send(Owner, new TEvTabletBase::TEvBlockBlobStorageResult(status, Info->TabletID, reason));
        PassAway();
    }

    void Handle(TEvTabletBase::TEvBlockBlobStorageResult::TPtr &ev) {
        auto *msg = ev->Get();
        auto it = Find(ReqActors, ev->Sender);
        Y_VERIFY(it != ReqActors.end(), "must not get response from unknown actor");
        *it = TActorId();

        switch (msg->Status) {
        case NKikimrProto::OK:
            if (++Replied == ReqActors.size())
                return ReplyAndDie(NKikimrProto::OK);
            break;
        default:
            return ReplyAndDie(msg->Status, msg->ErrorReason);
        }
    }
public:
    TTabletReqBlockBlobStorage(TActorId owner, TTabletStorageInfo *info, ui32 generation, bool blockPrevEntry)
        : Owner(owner)
        , Info(info)
        , Generation(generation)
        , BlockPrevEntry(blockPrevEntry)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_BLOCK_BS;
    }

    void Bootstrap() {
        TSet<ui32> blocked;

        const ui64 tabletId = Info->TabletID;
        ReqActors.reserve(Info->Channels.size());
        for (auto &x : Info->Channels) {
            if (auto *g = x.LatestEntry()) {
                if (blocked.insert(g->GroupID).second)
                    ReqActors.push_back(RegisterWithSameMailbox(new TTabletReqBlockBlobStorageGroup(SelfId(), tabletId, g->GroupID, Generation)));
            }

            if (BlockPrevEntry) {
                if (auto *pg = x.PreviousEntry())
                    if (blocked.insert(pg->GroupID).second)
                        ReqActors.push_back(RegisterWithSameMailbox(new TTabletReqBlockBlobStorageGroup(SelfId(), tabletId, pg->GroupID, Generation)));
            }
        }

        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvBlockBlobStorageResult, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateTabletReqBlockBlobStorage(const TActorId &owner, TTabletStorageInfo *info, ui32 generation, bool blockPrevEntry) {
    return new TTabletReqBlockBlobStorage(owner, info, generation, blockPrevEntry);
}

}
