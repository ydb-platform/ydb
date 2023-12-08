#include "services.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

class TExampleLookupRequestActor : public TActor<TExampleLookupRequestActor> {
    const TActorId Owner;
    const TActorId Replica;
    const TString Key;

    void Registered(TActorSystem* sys, const TActorId&) override {
        const auto flags = IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession;
        sys->Send(new IEventHandle(Replica, SelfId(), new TEvExample::TEvReplicaLookup(Key), flags));
    }

    void PassAway() override {
        const ui32 replicaNode = Replica.NodeId();
        if (replicaNode != SelfId().NodeId()) {
            const TActorId &interconnectProxy = TlsActivationContext->ExecutorThread.ActorSystem->InterconnectProxy(Replica.NodeId());
            Send(interconnectProxy, new TEvents::TEvUnsubscribe());
        }
        return IActor::PassAway();
    }

    void Handle(TEvExample::TEvReplicaInfo::TPtr &ev) {
        Send(Owner, ev->Release().Release());
        return PassAway();
    }

    void HandleUndelivered() {
        Send(Owner, new TEvExample::TEvReplicaInfo(Key));
        return PassAway();
    }
public:
    static constexpr IActor::EActivityType ActorActivityType() {
        // define app-specific activity tag to track elapsed cpu | handled events | actor count in Solomon
        return EActorActivity::ACTORLIB_COMMON;
    }

    TExampleLookupRequestActor(TActorId owner, TActorId replica, const TString &key)
        : TActor(&TThis::StateWork)
        , Owner(owner)
        , Replica(replica)
        , Key(key)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExample::TEvReplicaInfo, Handle);
            sFunc(TEvents::TEvUndelivered, HandleUndelivered);
            sFunc(TEvInterconnect::TEvNodeDisconnected, HandleUndelivered);
        default:
            break;
        }
    }
};

class TExampleLookupActor : public TActorBootstrapped<TExampleLookupActor> {
    TIntrusiveConstPtr<TExampleStorageConfig> Config;
    const TString Key;
    const TActorId ReplyTo;
    TVector<TActorId> RequestActors;

    ui32 TotalReplicas = 0;
    ui32 RepliedSuccess = 0;
    ui32 RepliedError = 0;

    TSet<TString> Payloads;

    void Handle(TEvExample::TEvReplicaInfo::TPtr &ev) {
        NActorsExample::TEvReplicaInfo &record = ev->Get()->Record;
        if (record.PayloadSize()) {
            ++RepliedSuccess;
            for (const TString &payload : record.GetPayload()) {
                Payloads.insert(payload);
            }
        }
        else {
            ++RepliedError;
        }

        const ui32 majority = (TotalReplicas / 2 + 1);
        if (RepliedSuccess == majority || (RepliedSuccess + RepliedError == TotalReplicas))
            return ReplyAndDie();
    }

    void ReplyAndDie() {
        TVector<TString> replyPayloads(Payloads.begin(), Payloads.end());
        Send(ReplyTo, new TEvExample::TEvInfo(Key, std::move(replyPayloads)));
        return PassAway();
    }
public:
    static constexpr IActor::EActivityType ActorActivityType() {
        // define app-specific activity tag to track elapsed cpu | handled events | actor count in Solomon
        return EActorActivity::ACTORLIB_COMMON;
    }

    TExampleLookupActor(TExampleStorageConfig *config, const TString &key, TActorId replyTo)
        : Config(config)
        , Key(key)
        , ReplyTo(replyTo)
    {}

    void Bootstrap() {
        Y_ABORT_UNLESS(Config->Replicas.size() > 0);

        TotalReplicas = Config->Replicas.size();
        RequestActors.reserve(TotalReplicas);
        for (const auto &replica : Config->Replicas) {
            const TActorId requestActor = Register(new TExampleLookupRequestActor(SelfId(), replica, Key));
            RequestActors.emplace_back(requestActor);
        }

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExample::TEvReplicaInfo, Handle);
        default:
            break;
        }
    }
};

IActor* CreateLookupActor(TExampleStorageConfig *config, const TString &key, TActorId replyTo) {
    return new TExampleLookupActor(config, key, replyTo);
}
