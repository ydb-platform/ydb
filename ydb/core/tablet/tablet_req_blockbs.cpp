#include "tablet_impl.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/set.h>

namespace NKikimr {

class TTabletReqBlockBlobStorageGroup : public TActorBootstrapped<TTabletReqBlockBlobStorageGroup> {
public:
    TActorId Owner;
    ui64 TabletId;
    ui32 GroupId;
    ui32 Generation;
    ui64 IssuerGuid;

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason = { }) {
        Send(Owner, new TEvTabletBase::TEvBlockBlobStorageResult(status, TabletId, reason));
        PassAway();
    }

    void SendRequest() {
        const TActorId proxy = MakeBlobStorageProxyID(GroupId);
        auto event = MakeHolder<TEvBlobStorage::TEvBlock>(TabletId, Generation, TInstant::Max(), IssuerGuid);
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

    TTabletReqBlockBlobStorageGroup(ui64 tabletId, ui32 groupId, ui32 gen, ui64 issuerGuid)
        : TabletId(tabletId)
        , GroupId(groupId)
        , Generation(gen)
        , IssuerGuid(issuerGuid)
    {}
};

class TTabletReqBlockBlobStorage : public TActorBootstrapped<TTabletReqBlockBlobStorage> {
    TActorId Owner;
    ui64 TabletId;
    ui32 Generation;
    ui32 Replied = 0;
    TVector<THolder<TTabletReqBlockBlobStorageGroup>> Requests;
    TVector<TActorId> ReqActors;
    ui64 IssuerGuid = RandomNumber<ui64>() | 1;

    void PassAway() override {
        for (auto &x : ReqActors)
            if (x)
                Send(x, new TEvents::TEvPoisonPill());

        TActor::PassAway();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TString &reason = { }) {
        Send(Owner, new TEvTabletBase::TEvBlockBlobStorageResult(status, TabletId, reason));
        PassAway();
    }

    void Handle(TEvTabletBase::TEvBlockBlobStorageResult::TPtr &ev) {
        auto *msg = ev->Get();
        auto it = Find(ReqActors, ev->Sender);
        Y_ABORT_UNLESS(it != ReqActors.end(), "must not get response from unknown actor");
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
    TTabletReqBlockBlobStorage(TActorId owner, TTabletStorageInfo* info, ui32 generation, bool blockPrevEntry)
        : Owner(owner)
        , TabletId(info->TabletID)
        , Generation(generation)
    {
        std::unordered_set<ui32> blocked;
        Requests.reserve(blockPrevEntry ? info->Channels.size() * 2 : info->Channels.size());
        for (auto& channel : info->Channels) {
            if (channel.History.empty()) {
                continue;
            }
            auto itEntry = channel.History.rbegin();
            while (itEntry != channel.History.rend() && itEntry->FromGeneration > generation) {
                ++itEntry;
            }
            if (itEntry == channel.History.rend()) {
                continue;
            }
            if (blocked.insert(itEntry->GroupID).second) {
                Requests.emplace_back(new TTabletReqBlockBlobStorageGroup(TabletId, itEntry->GroupID, Generation,
                    IssuerGuid));
            }

            if (blockPrevEntry) {
                ++itEntry;
                if (itEntry == channel.History.rend()) {
                    continue;
                }
                if (blocked.insert(itEntry->GroupID).second) {
                    Requests.emplace_back(new TTabletReqBlockBlobStorageGroup(TabletId, itEntry->GroupID, Generation,
                        IssuerGuid));
                }
            }
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_BLOCK_BS;
    }

    void Bootstrap() {
        ReqActors.reserve(Requests.size());
        for (auto& req : Requests) {
            req->Owner = SelfId();
            ReqActors.push_back(RegisterWithSameMailbox(req.Release()));
        }
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletBase::TEvBlockBlobStorageResult, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateTabletReqBlockBlobStorage(const TActorId& owner, TTabletStorageInfo* info, ui32 generation, bool blockPrevEntry) {
    return new TTabletReqBlockBlobStorage(owner, info, generation, blockPrevEntry);
}

}
