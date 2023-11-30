#include "grpc_endpoint.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

class TGrpcPublisherServiceActor : public TActorBootstrapped<TGrpcPublisherServiceActor> {
    TVector<TIntrusivePtr<TGrpcEndpointDescription>> Endpoints;
    TVector<TActorId> ActorIds;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_ENDPOINT_PUBLISH;
    }

    TGrpcPublisherServiceActor(TVector<TIntrusivePtr<TGrpcEndpointDescription>>&& endpoints)
        : Endpoints(endpoints)
    {}

    void Bootstrap() {
        ActorIds.reserve(Endpoints.size());
        for(auto& endpoint: Endpoints) {
            auto actor = CreateGrpcEndpointPublishActor(endpoint.Get());
            ActorIds.push_back(Register(actor));
        }

        Become(&TThis::StateWork);
    }

    void PassAway() override {
        for(auto& actorId : ActorIds) {
            Send(actorId, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateGrpcPublisherServiceActor(TVector<TIntrusivePtr<TGrpcEndpointDescription>>&& endpoints) {
    return new TGrpcPublisherServiceActor(std::move(endpoints));
}

} // namespace NKikimr::NGRpcService
