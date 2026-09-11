#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/iam/operation_service.grpc.pb.h>
#include "operation_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/ycloud/impl/util.h>

namespace NCloud {

using namespace NKikimr;

TOperationServiceSettings::TOperationServiceSettings(TString endpoint, TStringBuf userAgentHint) {
    Endpoint = std::move(endpoint);
    UserAgentPrefix = BuildUserAgentPrefix(userAgentHint);
}

class TOperationService : public NActors::TActor<TOperationService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::iam::v1::OperationService> {
    using TThis = TOperationService;
    using TBase = NActors::TActor<TOperationService>;

    struct TGetOperationRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::OperationService::Stub::AsyncGet;
        using TRequestEventType = TEvOperationService::TEvGetOperationRequest;
        using TResponseEventType = TEvOperationService::TEvGetOperationResponse;
    };

    void Handle(TEvOperationService::TEvGetOperationRequest::TPtr& ev) {
        MakeCall<TGetOperationRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::IAM_OPERATION_SERVICE_ACTOR; }

    TOperationService(const TOperationServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvOperationService::TEvGetOperationRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

IActor* CreateOperationService(const TOperationServiceSettings& settings) {
    return new TOperationService(settings);
}

}
