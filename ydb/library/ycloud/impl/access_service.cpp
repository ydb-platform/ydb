#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/json/json_value.h>
#include <ydb/public/api/client/yc_private/accessservice/access_service.grpc.pb.h>
#include "access_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ycloud/impl/util.h>

namespace NCloud {

using namespace NKikimr;

TAccessServiceSettings::TAccessServiceSettings(TString endpoint, TStringBuf userAgentHint) {
    Endpoint = std::move(endpoint);
    UserAgentPrefix = BuildUserAgentPrefix(userAgentHint);
}

class TAccessService : public NActors::TActor<TAccessService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::accessservice::v2::AccessService> {
    using TThis = TAccessService;
    using TBase = NActors::TActor<TAccessService>;

    struct TAuthenticateRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncAuthenticate;
        using TRequestEventType = TEvAccessService::TEvAuthenticateRequest;
        using TResponseEventType = TEvAccessService::TEvAuthenticateResponse;

        static yandex::cloud::priv::accessservice::v2::AuthenticateRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthenticateRequest& request) {
            yandex::cloud::priv::accessservice::v2::AuthenticateRequest result(request);
            if (result.iam_token()) {
                result.set_iam_token(MaskToken(result.iam_token()));
            }
            if (result.api_key()) {
                result.set_api_key(MaskToken(result.api_key()));
            }
            if (result.refresh_token()) {
                result.set_refresh_token(MaskToken(result.refresh_token()));
            }
            result.clear_iam_cookie();
            return result;
        }

        static const yandex::cloud::priv::accessservice::v2::AuthenticateResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthenticateResponse& response) {
            return response;
        }
    };

    void Handle(TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        MakeCall<TAuthenticateRequest>(std::move(ev));
    }

    struct TAuthorizeRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncAuthorize;
        using TRequestEventType = TEvAccessService::TEvAuthorizeRequest;
        using TResponseEventType = TEvAccessService::TEvAuthorizeResponse;

        static yandex::cloud::priv::accessservice::v2::AuthorizeRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthorizeRequest& request) {
            yandex::cloud::priv::accessservice::v2::AuthorizeRequest result(request);
            if (result.iam_token()) {
                result.set_iam_token(MaskToken(result.iam_token()));
            }
            if (result.api_key()) {
                result.set_api_key(MaskToken(result.api_key()));
            }
            return result;
        }

        static const yandex::cloud::priv::accessservice::v2::AuthorizeResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthorizeResponse& response) {
            return response;
        }
    };

    void Handle(TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        MakeCall<TAuthorizeRequest>(std::move(ev));
    }

    struct TBulkAuthorizeRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncBulkAuthorize;
        using TRequestEventType = TEvAccessService::TEvBulkAuthorizeRequest;
        using TResponseEventType = TEvAccessService::TEvBulkAuthorizeResponse;

        static yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest& request) {
            yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest result(request);
            if (result.iam_token()) {
                result.set_iam_token(MaskToken(result.iam_token()));
            }
            if (result.api_key()) {
                result.set_api_key(MaskToken(result.api_key()));
            }
            return result;
        }

        static const yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse& response) {
            return response;
        }
    };

    void Handle(TEvAccessService::TEvBulkAuthorizeRequest::TPtr& ev) {
        MakeCall<TBulkAuthorizeRequest>(std::move(ev));
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
            hFunc(TEvAccessService::TEvBulkAuthorizeRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateAccessService(const TAccessServiceSettings& settings) {
    return new TAccessService(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings) {
    IActor* accessService = CreateAccessService(settings);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<TEvAccessService::TEvAuthenticateRequest, TEvAccessService::TEvAuthenticateResponse>(accessService);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<TEvAccessService::TEvAuthorizeRequest, TEvAccessService::TEvAuthorizeResponse>(accessService);
    return accessService;
}

}
