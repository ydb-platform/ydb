#include "statestorage_impl.h"
#include "tabletid.h"
#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>

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
        Send(PublishActor, new TEvStateStorage::TEvPublishActorGone(Replica));
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
    struct TEvPrivate {
        enum EEv {
            EvRetryPublishActor = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvRetryPublishActor :
                public TEventLocal<TEvRetryPublishActor, EEv::EvRetryPublishActor> {
            TActorId Replica;

            TEvRetryPublishActor(TActorId replica) : Replica(replica) {
            }
        };
    };

    const TString Path;
    const TString Payload;
    const TActorId Owner;
    const ui32 TtlMs;
    const bool Register;
    const TBoardRetrySettings BoardRetrySettings;

    struct TRetryState {
        NMonotonic::TMonotonic LastRetryAt = TMonotonic::Zero();
        TDuration CurrentDelay = TDuration::Zero();
    };

    struct TReplicaPublishActorState {
        TRetryState RetryState;
        TActorId PublishActor;
    };

    THashMap<TActorId, TReplicaPublishActorState> ReplicaPublishActors; // replica -> publish actor

    const TDuration& GetCurrentDelay(TRetryState& state) {
        if (state.CurrentDelay == TDuration::Zero()) {
            state.CurrentDelay = BoardRetrySettings.StartDelayMs;
        }
        return state.CurrentDelay;
    }

    TDuration GetRetryDelay(TRetryState& state) {
        auto newDelay = state.CurrentDelay;
        newDelay *= 2;
        if (newDelay > BoardRetrySettings.MaxDelayMs) {
            newDelay = BoardRetrySettings.MaxDelayMs;
        }
        newDelay *= AppData()->RandomProvider->Uniform(50, 200);
        newDelay /= 100;
        state.CurrentDelay = newDelay;
        return state.CurrentDelay;
    }

    void PassAway() override {
        for (auto &xpair : ReplicaPublishActors) {
            if (xpair.second.PublishActor)
                Send(xpair.second.PublishActor, new TEvents::TEvPoisonPill());
        }

        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeStateStorageProxyID(), SelfId(),
            nullptr, 0));

        TActor::PassAway();
    }

    void HandleUndelivered() {
        BLOG_ERROR("publish on unavailable statestorage board service");
        Become(&TThis::StateCalm);
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev) {
        auto *msg = ev->Get();

        if (msg->Replicas.empty()) {
            Y_ABORT_UNLESS(ReplicaPublishActors.empty());
            BLOG_ERROR("publish on unconfigured statestorage board service");
        } else {
            auto now = TlsActivationContext->Monotonic();

            for (auto &replicaId : msg->Replicas) {
                auto& publishActorState = ReplicaPublishActors[replicaId];
                if (publishActorState.RetryState.LastRetryAt == TMonotonic::Zero()) {
                    publishActorState.PublishActor =
                        RegisterWithSameMailbox(new TBoardReplicaPublishActor(Path, Payload, replicaId, SelfId()));
                    publishActorState.RetryState.LastRetryAt = now;
                }
            }
            THashSet<TActorId> usedReplicas(msg->Replicas.begin(), msg->Replicas.end());
            for (auto it = ReplicaPublishActors.begin(); it != ReplicaPublishActors.end(); ) {
                if (usedReplicas.contains(it->first)) {
                    ++it;
                } else {
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, it->second.PublishActor,
                        SelfId(), nullptr, 0));
                    ReplicaPublishActors.erase(it++);
                }
            }
        }

        Become(&TThis::StateCalm);
    }

    bool ResolveGone(TEvStateStorage::TEvPublishActorGone::TPtr &ev) {
        const TActorId sender = ev->Sender;
        const TActorId replica = ev->Get()->Replica;

        auto replicaPublishActorsIt = ReplicaPublishActors.find(replica);
        if (replicaPublishActorsIt == ReplicaPublishActors.end()) {
            return false;
        }

        if (replicaPublishActorsIt->second.PublishActor == sender) {
            replicaPublishActorsIt->second.PublishActor = TActorId();
            return true;
        }

        return false;
    }

    void CalmGone(TEvStateStorage::TEvPublishActorGone::TPtr &ev) {
        if (ResolveGone(ev)) {
            auto now = TlsActivationContext->Monotonic();

            const TActorId replica = ev->Get()->Replica;
            auto& retryState = ReplicaPublishActors[replica].RetryState;

            if (now - retryState.LastRetryAt < GetCurrentDelay(retryState)) {
                auto at = retryState.LastRetryAt + GetRetryDelay(retryState);
                Schedule(at - now, new TEvPrivate::TEvRetryPublishActor(replica));
                return;
            }

            RetryReplica(replica);
        }
    }

    void Handle(TEvPrivate::TEvRetryPublishActor::TPtr &ev) {
        const auto& replica = ev->Get()->Replica;

        RetryReplica(replica, true);
    }


    void RetryReplica(const TActorId& replica, bool fromRetry = false) {
        if (!fromRetry) {
            auto delay = TDuration::Seconds(2);
            delay *= AppData()->RandomProvider->Uniform(50, 200);
            delay /= 100;
            Schedule(delay, new TEvPrivate::TEvRetryPublishActor(replica));
            return;
       }

        auto replicaPublishActorsIt = ReplicaPublishActors.find(replica);
        if (replicaPublishActorsIt == ReplicaPublishActors.end()) {
            return;
        }

        if (replicaPublishActorsIt->second.PublishActor != TActorId()) {
            return;
        }

        auto now = TlsActivationContext->Monotonic();
        replicaPublishActorsIt->second.RetryState.LastRetryAt = now;
        replicaPublishActorsIt->second.PublishActor =
            RegisterWithSameMailbox(new TBoardReplicaPublishActor(Path, Payload, replica, SelfId()));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BOARD_PUBLISH_ACTOR;
    }

    TBoardPublishActor(
        const TString &path, const TString &payload, const TActorId &owner, ui32 ttlMs, bool reg,
        TBoardRetrySettings boardRetrySettings)
        : Path(path)
        , Payload(payload)
        , Owner(owner)
        , TtlMs(ttlMs)
        , Register(reg)
        , BoardRetrySettings(std::move(boardRetrySettings))
    {
        Y_UNUSED(TtlMs);
        Y_UNUSED(Register);
    }

    void Bootstrap() {
        const TActorId proxyId = MakeStateStorageProxyID();
        Send(proxyId, new TEvStateStorage::TEvResolveBoard(Path, true), IEventHandle::FlagTrackDelivery);

        Become(&TThis::StateResolve);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            hFunc(TEvStateStorage::TEvPublishActorGone, CalmGone);
            hFunc(TEvPrivate::TEvRetryPublishActor, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBoardPublishActor(
        const TString &path, const TString &payload, const TActorId &owner, ui32 ttlMs, bool reg,
        TBoardRetrySettings boardRetrySettings) {
    return new TBoardPublishActor(path, payload, owner, ttlMs, reg, std::move(boardRetrySettings));
}

TString MakeEndpointsBoardPath(const TString &database) {
    return "gpc+" + database;
}

}
