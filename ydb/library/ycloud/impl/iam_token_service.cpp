#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include "iam_token_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ycloud/impl/util.h>

namespace NCloud {

using namespace NKikimr;

TIamTokenServiceSettings::TIamTokenServiceSettings(TString endpoint, TStringBuf userAgentHint) {
    Endpoint = std::move(endpoint);
    UserAgentPrefix = BuildUserAgentPrefix(userAgentHint);
}

class TIamTokenService : public NActors::TActor<TIamTokenService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::iam::v1::IamTokenService> {
    using TThis = TIamTokenService;
    using TBase = NActors::TActor<TIamTokenService>;

    struct TCreateIamTokenForServiceAccountRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreateForServiceAccount;
        using TRequestEventType = TEvIamTokenService::TEvCreateForServiceAccountRequest;
        using TResponseEventType = TEvIamTokenService::TEvCreateResponse;
    };

    void Handle(TEvIamTokenService::TEvCreateForServiceAccountRequest::TPtr& ev) {
        MakeCall<TCreateIamTokenForServiceAccountRequest>(std::move(ev));
    }

    struct TCreateIamTokenForServiceRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::IamTokenService::Stub::AsyncCreateForService;
        using TRequestEventType = TEvIamTokenService::TEvCreateForServiceRequest;
        using TResponseEventType = TEvIamTokenService::TEvCreateForServiceResponse;

        static const yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest& Obfuscate(const yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest& p) {
            return p;
        }

        static yandex::cloud::priv::iam::v1::CreateIamTokenResponse Obfuscate(const yandex::cloud::priv::iam::v1::CreateIamTokenResponse& p) {
            yandex::cloud::priv::iam::v1::CreateIamTokenResponse r(p);
            if (r.iam_token()) {
                r.set_iam_token(MaskToken(r.iam_token()));
            }
            return r;
        }
    };

    void Handle(TEvIamTokenService::TEvCreateForServiceRequest::TPtr& ev) {
        MakeCall<TCreateIamTokenForServiceRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::IAM_TOKEN_SERVICE_ACTOR; }

    TIamTokenService(const TIamTokenServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvIamTokenService::TEvCreateForServiceAccountRequest, Handle);
            hFunc(TEvIamTokenService::TEvCreateForServiceRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateIamTokenService(const TIamTokenServiceSettings& settings) {
    return new TIamTokenService(settings);
}

IActor* CreateIamTokenServiceWithCache(const TIamTokenServiceSettings& settings) {
    IActor* iamTokenService = CreateIamTokenService(settings);
    iamTokenService = NGrpcActorClient::CreateGrpcServiceCache<TEvIamTokenService::TEvCreateForServiceAccountRequest, TEvIamTokenService::TEvCreateResponse>(iamTokenService);
    return iamTokenService;
}

}
