#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/json/json_value.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include "access_service.h"
#include "grpc_service_client.h"
#include "grpc_service_cache.h"

namespace NCloud {

using namespace NKikimr;

class TAccessService : public NActors::TActor<TAccessService>, TGrpcServiceClient<yandex::cloud::priv::servicecontrol::v1::AccessService> {
    using TThis = TAccessService;
    using TBase = NActors::TActor<TAccessService>;

    struct TAuthenticateRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::servicecontrol::v1::AccessService::Stub::AsyncAuthenticate;
        using TRequestEventType = TEvAccessService::TEvAuthenticateRequest;
        using TResponseEventType = TEvAccessService::TEvAuthenticateResponse;

        static yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest& p) {
            yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest r(p);
            if (r.iam_token()) {
                r.set_iam_token(MaskToken(r.iam_token()));
            }
            if (r.api_key()) {
                r.set_api_key(MaskToken(r.api_key()));
            }
            r.clear_iam_cookie();
            return r;
        }

        static const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        MakeCall<TAuthenticateRequest>(std::move(ev));
    }

    struct TAuthorizeRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::servicecontrol::v1::AccessService::Stub::AsyncAuthorize;
        using TRequestEventType = TEvAccessService::TEvAuthorizeRequest;
        using TResponseEventType = TEvAccessService::TEvAuthorizeResponse;

        static yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest& p) {
            yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest r(p);
            if (r.iam_token()) {
                r.set_iam_token(MaskToken(r.iam_token()));
            }
            if (r.api_key()) {
                r.set_api_key(MaskToken(r.api_key()));
            }
            return r;
        }

        static const yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse& Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        MakeCall<TAuthorizeRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::ACCESS_SERVICE_ACTOR; }

    TAccessService(const TAccessServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequest, Handle);
            hFunc(TEvAccessService::TEvAuthorizeRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateAccessService(const TAccessServiceSettings& settings) {
    return new TAccessService(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings) {
    IActor* accessService = CreateAccessService(settings);
    accessService = CreateGrpcServiceCache<TEvAccessService::TEvAuthenticateRequest, TEvAccessService::TEvAuthenticateResponse>(accessService);
    accessService = CreateGrpcServiceCache<TEvAccessService::TEvAuthorizeRequest, TEvAccessService::TEvAuthorizeResponse>(accessService);
    return accessService;
}

}
