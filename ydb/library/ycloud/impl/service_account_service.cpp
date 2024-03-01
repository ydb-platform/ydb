#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/iam/service_account_service.grpc.pb.h>
#include "service_account_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

class TServiceAccountService : public NActors::TActor<TServiceAccountService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::iam::v1::ServiceAccountService> {
    using TThis = TServiceAccountService;
    using TBase = NActors::TActor<TServiceAccountService>;

    struct TGetServiceAccountRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::ServiceAccountService::Stub::AsyncGet;
        using TRequestEventType = TEvServiceAccountService::TEvGetServiceAccountRequest;
        using TResponseEventType = TEvServiceAccountService::TEvGetServiceAccountResponse;
    };

    void Handle(TEvServiceAccountService::TEvGetServiceAccountRequest::TPtr& ev) {
        MakeCall<TGetServiceAccountRequest>(std::move(ev));
    }

    struct TIssueTokenRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::ServiceAccountService::Stub::AsyncIssueToken;
        using TRequestEventType = TEvServiceAccountService::TEvIssueTokenRequest;
        using TResponseEventType = TEvServiceAccountService::TEvIssueTokenResponse;

        static const yandex::cloud::priv::iam::v1::IssueTokenRequest& Obfuscate(const yandex::cloud::priv::iam::v1::IssueTokenRequest& p) {
            return p;
        }

        static yandex::cloud::priv::iam::v1::IamToken Obfuscate(const yandex::cloud::priv::iam::v1::IamToken& p) {
            yandex::cloud::priv::iam::v1::IamToken r(p);
            if (r.iam_token()) {
                r.set_iam_token(MaskToken(r.iam_token()));
            }
            return r;
        }
    };

    void Handle(TEvServiceAccountService::TEvIssueTokenRequest::TPtr& ev) {
        MakeCall<TIssueTokenRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::SERVICE_ACCOUNT_SERVICE_ACTOR; }

    TServiceAccountService(const TServiceAccountServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvServiceAccountService::TEvGetServiceAccountRequest, Handle);
            hFunc(TEvServiceAccountService::TEvIssueTokenRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings) {
    return new TServiceAccountService(settings);
}

}
