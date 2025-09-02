#include "access_service.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/public/api/client/nc_private/iam/v1/access_service.grpc.pb.h>

namespace NNebiusCloud {

using namespace NKikimr;

class TAccessServiceV1 : public NActors::TActor<TAccessServiceV1>, NGrpcActorClient::TGrpcServiceClient<nebius::iam::v1::AccessService> {
    using TThis = TAccessServiceV1;
    using TBase = NActors::TActor<TAccessServiceV1>;

    struct TAuthenticateRequest : TGrpcRequest {
        static constexpr auto Request = &nebius::iam::v1::AccessService::Stub::AsyncAuthenticate;
        using TRequestEventType = TEvAccessService::TEvAuthenticateRequest;
        using TResponseEventType = TEvAccessService::TEvAuthenticateResponse;

        static nebius::iam::v1::AuthenticateRequest Obfuscate(const nebius::iam::v1::AuthenticateRequest& p) {
            nebius::iam::v1::AuthenticateRequest r(p);
            if (r.iam_token()) {
                r.set_iam_token(MaskToken(r.iam_token()));
            }
            return r;
        }

        static const nebius::iam::v1::AuthenticateResponse& Obfuscate(const nebius::iam::v1::AuthenticateResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        MakeCall<TAuthenticateRequest>(std::move(ev));
    }

    struct TAuthorizeRequest : TGrpcRequest {
        static constexpr auto Request = &nebius::iam::v1::AccessService::Stub::AsyncAuthorize;
        using TRequestEventType = TEvAccessService::TEvAuthorizeRequest;
        using TResponseEventType = TEvAccessService::TEvAuthorizeResponse;

        static nebius::iam::v1::AuthorizeRequest Obfuscate(const nebius::iam::v1::AuthorizeRequest& p) {
            nebius::iam::v1::AuthorizeRequest r(p);
            for (auto& [_, check] : *r.mutable_checks()) {
                if (check.iam_token()) {
                    check.set_iam_token(MaskToken(check.iam_token()));
                }
            }
            return r;
        }

        static const nebius::iam::v1::AuthorizeResponse& Obfuscate(const nebius::iam::v1::AuthorizeResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        MakeCall<TAuthorizeRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::ACCESS_SERVICE_ACTOR; }

    TAccessServiceV1(const TAccessServiceSettings& settings)
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

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings) {
    return new TAccessServiceV1(settings);
}

} // namespace NNebiusCloud
