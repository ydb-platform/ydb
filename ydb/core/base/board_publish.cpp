#include "statestorage_impl.h"
#include "tabletid.h"
#include <ydb/core/protos/services.pb.h>
#include <library/cpp/actors/core/interconnect.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/map.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOARD_PUBLISH, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOARD_PUBLISH, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BOARD_PUBLISH, stream)

namespace NKikimr {

class TBoardReplicaPublishActor : public TActorBootstrapped<TBoardReplicaPublishActor> {
    const TString Path;
    TString Payload;
    const TActorId Replica;
    const TActorId PublishActor;

    ui64 Round;

    void Cleanup() {
        Send(Replica, new TEvStateStorage::TEvReplicaBoardCleanup());
        if (Replica.NodeId() != SelfId().NodeId())
            Send(TActivationContext::InterconnectProxy(Replica.NodeId()), new TEvents::TEvUnsubscribe());
        PassAway();
    }

    void NotAvailable() {
        Send(PublishActor, new TEvents::TEvGone());
        PassAway();
    }

    void NotAvailableUnsubscribe() {
        if (Replica.NodeId() != SelfId().NodeId())
            Send(TActivationContext::InterconnectProxy(Replica.NodeId()), new TEvents::TEvUnsubscribe());
        Send(PublishActor, new TEvents::TEvGone());
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_REPLICA_PUBLISH_ACTOR;
    }

    TBoardReplicaPublishActor(const TString &path, const TString &payload, TActorId replica, TActorId publishActor)
        : Path(path)
        , Payload(payload)
        , Replica(replica)
        , PublishActor(publishActor)
        , Round(0)
    {}

    void Bootstrap() {
        // Note: we don't track delivery, and instead treat undelivery as some
        // form of silent "permanent" failure, waiting for disconnection. On
        // disconnection we assume the node may be restarted with a new
        // configuration and the actor become valid.
        Send(Replica, new TEvStateStorage::TEvReplicaBoardPublish(Path, Payload, 0, true, PublishActor), IEventHandle::FlagSubscribeOnSession, ++Round);

        Become(&TThis::StatePublish);
    }

    STATEFN(StatePublish) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, Cleanup);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, NotAvailable); // no cleanup on node disconnect
            cFunc(TEvStateStorage::TEvReplicaShutdown::EventType, NotAvailableUnsubscribe);
        }
    }
};

class TBoardPublishActor : public TActorBootstrapped<TBoardPublishActor> {
    const TString Path;
    const TString Payload;
    const TActorId Owner;
    const ui32 StateStorageGroupId;
    const ui32 TtlMs;
    const bool Register;

    TMap<TActorId, TActorId> ReplicaPublishActors; // replica -> publish actor

    void PassAway() override {
        for (auto &xpair : ReplicaPublishActors) {
            if (xpair.second)
                Send(xpair.second, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

    void HandleUndelivered() {
        BLOG_ERROR("publish on unavailable statestorage board service " << StateStorageGroupId);
        Become(&TThis::StateCalm);
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev) {
        auto *msg = ev->Get();

        if (msg->Replicas.empty()) {
            BLOG_ERROR("publish on unconfigured statestorage board service " << StateStorageGroupId);
        } else {
            TMap<TActorId, TActorId> updated;

            for (auto &replicaId : msg->Replicas) {
                const TActorId *known = ReplicaPublishActors.FindPtr(replicaId);
                if (known && *known)
                    updated[replicaId] = *known;
                else
                    updated[replicaId] = RegisterWithSameMailbox(new TBoardReplicaPublishActor(Path, Payload, replicaId, SelfId()));
            }

            ReplicaPublishActors = std::move(updated);
        }

        Become(&TThis::StateCalm);
    }

    bool ResolveGone(TEvents::TEvGone::TPtr &ev) {
        const TActorId sender = ev->Sender;

        for (auto &xpair : ReplicaPublishActors) {
            if (xpair.second == sender) {
                xpair.second = TActorId();
                return true;
            }
        }

        return false;
    }

    void CalmGone(TEvents::TEvGone::TPtr &ev) {
        if (ResolveGone(ev)) {
            const TActorId proxyId = MakeStateStorageProxyID(StateStorageGroupId);
            const ui32 flags = IEventHandle::FlagTrackDelivery;
            TAutoPtr<IEventHandle> x = new IEventHandle(proxyId, SelfId(), new TEvStateStorage::TEvResolveBoard(Path), flags);
            TActivationContext::Schedule(TDuration::MilliSeconds(50), x.Release());

            Become(&TThis::StateResolve);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_PUBLISH_ACTOR;
    }

    TBoardPublishActor(const TString &path, const TString &payload, const TActorId &owner, ui32 groupId, ui32 ttlMs, bool reg)
        : Path(path)
        , Payload(payload)
        , Owner(owner)
        , StateStorageGroupId(groupId)
        , TtlMs(ttlMs)
        , Register(reg)
    {
        Y_UNUSED(TtlMs);
        Y_UNUSED(Register);
    }

    void Bootstrap() {
        const TActorId proxyId = MakeStateStorageProxyID(StateStorageGroupId);
        Send(proxyId, new TEvStateStorage::TEvResolveBoard(Path), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateResolve);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TEvents::TEvGone, ResolveGone);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvGone, CalmGone);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBoardPublishActor(const TString &path, const TString &payload, const TActorId &owner, ui32 groupId, ui32 ttlMs, bool reg) {
    return new TBoardPublishActor(path, payload, owner, groupId, ttlMs, reg);
}

TString MakeEndpointsBoardPath(const TString &database) {
    return "gpc+" + database;
}

}
