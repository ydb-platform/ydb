#include "grpc_endpoint.h"

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

class TGrpcPublisherServiceActor : public TActorBootstrapped<TGrpcPublisherServiceActor> {
    TVector<TIntrusivePtr<TGrpcEndpointDescription>> Endpoints;
    TVector<TActorId> ActorIds;
    const TDuration WarmupTimeout;
    bool WarmupGatePassed = false;

    struct TEvPrivate {
        enum EEv {
            EvWarmupTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvWarmupTimeout : TEventLocal<TEvWarmupTimeout, EvWarmupTimeout> {};
    };

    void StartPublishing() {
        if (WarmupGatePassed) {
            return;
        }
        WarmupGatePassed = true;

        ActorIds.reserve(Endpoints.size());
        for (auto& endpoint : Endpoints) {
            auto actor = CreateGrpcEndpointPublishActor(endpoint.Get());
            ActorIds.push_back(Register(actor));
        }

        Become(&TThis::StateWork);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_ENDPOINT_PUBLISH;
    }

    TGrpcPublisherServiceActor(TVector<TIntrusivePtr<TGrpcEndpointDescription>>&& endpoints,
                               TDuration warmupTimeout)
        : Endpoints(std::move(endpoints))
        , WarmupTimeout(warmupTimeout)
    {}

    void Bootstrap() {
        if (WarmupTimeout == TDuration::Zero()) {
            StartPublishing();
            return;
        }

        Schedule(WarmupTimeout, new TEvPrivate::TEvWarmupTimeout());
        Become(&TThis::StateWaitWarmup);
    }

    void PassAway() override {
        for(auto& actorId : ActorIds) {
            Send(actorId, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

    void HandleWarmupComplete() {
        StartPublishing();
    }

    void HandleWarmupTimeout() {
        StartPublishing();
    }

    STFUNC(StateWaitWarmup) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NKqp::TKqpEvents::EvWarmupComplete, HandleWarmupComplete);
            cFunc(TEvPrivate::EvWarmupTimeout, HandleWarmupTimeout);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateGrpcPublisherServiceActor(TVector<TIntrusivePtr<TGrpcEndpointDescription>>&& endpoints,
                                        TDuration warmupTimeout) {
    return new TGrpcPublisherServiceActor(std::move(endpoints), warmupTimeout);
}

} // namespace NKikimr::NGRpcService
