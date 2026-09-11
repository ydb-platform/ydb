#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/iam/service_control_service.grpc.pb.h>
#include "service_control_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/ycloud/impl/util.h>

namespace NCloud {

using namespace NKikimr;

TServiceControlServiceSettings::TServiceControlServiceSettings(TString endpoint, TStringBuf userAgentHint) {
    Endpoint = std::move(endpoint);
    UserAgentPrefix = BuildUserAgentPrefix(userAgentHint);
}

class TServiceControlService : public NActors::TActor<TServiceControlService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::iam::v1::ServiceControlService> {
    using TThis = TServiceControlService;
    using TBase = NActors::TActor<TServiceControlService>;

    struct TSetupDelegationRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::ServiceControlService::Stub::AsyncSetupDelegation;
        using TRequestEventType = TEvServiceControlService::TEvSetupDelegationRequest;
        using TResponseEventType = TEvServiceControlService::TEvSetupDelegationResponse;
    };

    void Handle(TEvServiceControlService::TEvSetupDelegationRequest::TPtr& ev) {
        MakeCall<TSetupDelegationRequest>(std::move(ev));
    }

    struct TRevokeDelegationRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::ServiceControlService::Stub::AsyncRevokeDelegation;
        using TRequestEventType = TEvServiceControlService::TEvRevokeDelegationRequest;
        using TResponseEventType = TEvServiceControlService::TEvRevokeDelegationResponse;
    };

    void Handle(TEvServiceControlService::TEvRevokeDelegationRequest::TPtr& ev) {
        MakeCall<TRevokeDelegationRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::SERVICE_CONTROL_SERVICE_ACTOR; }

    TServiceControlService(const TServiceControlServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvServiceControlService::TEvSetupDelegationRequest, Handle);
            hFunc(TEvServiceControlService::TEvRevokeDelegationRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

IActor* CreateServiceControlService(const TServiceControlServiceSettings& settings) {
    return new TServiceControlService(settings);
}

}
