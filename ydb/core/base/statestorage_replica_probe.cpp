#include "statestorage_impl.h"
#include "tabletid.h"
#include "appdata.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/set.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)

namespace NKikimr {

class TStateStorageReplicaProbe : public TActorBootstrapped<TStateStorageReplicaProbe> {
    const TActorId ReplicaId;

    TSet<TActorId> Subscribers;

    void PassAway() override {
        for (TActorId x : Subscribers)
            Send(x, new TEvStateStorage::TEvReplicaProbeDisconnected(ReplicaId));
        Subscribers.clear();
        Send(TActivationContext::InterconnectProxy(ReplicaId.NodeId()), new TEvents::TEvUnsubscribe());

        IActor::PassAway();
    }

    void HandleWait(TEvStateStorage::TEvReplicaProbeSubscribe::TPtr &ev) {
        Subscribers.emplace(ev->Sender);
    }

    void HandleConnected(TEvStateStorage::TEvReplicaProbeSubscribe::TPtr &ev) {
        Send(ev->Sender, new TEvStateStorage::TEvReplicaProbeConnected(ReplicaId));
    }

    void Handle(TEvStateStorage::TEvReplicaProbeUnsubscribe::TPtr &ev) {
        Subscribers.erase(ev->Sender);
    }

    void HandlePong() {
    // we can receive outdated pong, we don't care much, it's best effort
        for (TActorId x : Subscribers)
            Send(x, new TEvStateStorage::TEvReplicaProbeConnected(ReplicaId));
        Subscribers.clear();
    }

    void SomeSleep() {
        Become(&TThis::StateWait, TDuration::MilliSeconds(75), new TEvents::TEvWakeup());
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_PROXY;
    }

    void Bootstrap() {
        Send(ReplicaId, new TEvents::TEvPing(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateWait);
    }

    TStateStorageReplicaProbe(TActorId replicaId)
        : ReplicaId(replicaId)
    {}

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaProbeSubscribe, HandleWait);
            hFunc(TEvStateStorage::TEvReplicaProbeUnsubscribe, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            cFunc(TEvents::TEvPong::EventType, HandlePong);
            cFunc(TEvents::TEvWakeup::EventType, Bootstrap);

            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);
        }
    }

    STATEFN(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaProbeSubscribe, HandleConnected);
            hFunc(TEvStateStorage::TEvReplicaProbeUnsubscribe, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);

            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);
        }
    }
};

IActor* CreateStateStorageReplicaProbe(TActorId replica) {
    return new TStateStorageReplicaProbe(replica);
}

}
