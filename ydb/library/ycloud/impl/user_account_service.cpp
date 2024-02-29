#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/iam/user_account_service.grpc.pb.h>
#include "user_account_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

class TUserAccountService : public NActors::TActor<TUserAccountService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::iam::v1::UserAccountService> {
    using TThis = TUserAccountService;
    using TBase = NActors::TActor<TUserAccountService>;

    struct TGetUserAccountRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::iam::v1::UserAccountService::Stub::AsyncGet;
        using TRequestEventType = TEvUserAccountService::TEvGetUserAccountRequest;
        using TResponseEventType = TEvUserAccountService::TEvGetUserAccountResponse;
    };

    void Handle(TEvUserAccountService::TEvGetUserAccountRequest::TPtr& ev) {
        MakeCall<TGetUserAccountRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::USER_ACCOUNT_SERVICE_ACTOR; }

    TUserAccountService(const TUserAccountServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvUserAccountService::TEvGetUserAccountRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings) {
    return new TUserAccountService(settings);
}

}
