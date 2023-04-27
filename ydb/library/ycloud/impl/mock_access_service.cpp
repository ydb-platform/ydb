#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/json/json_value.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include "access_service.h"
#include "grpc_service_client.h"
#include "grpc_service_cache.h"

namespace NCloud {

using namespace NKikimr;

class TAccessServiceMock
    : public NActors::TActor<TAccessServiceMock> {
    using TThis = TAccessServiceMock;
    using TBase = NActors::TActor<TAccessServiceMock>;

public:
    TAccessServiceMock()
        : TBase(&TThis::StateWork) {
    }

    void Handle(TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthenticateResponse>();
        result->Response.mutable_subject()->mutable_user_account()->set_federation_id("mock");
        result->Status.Ok();
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthorizeResponse>();
        result->Status = NGrpc::TGrpcStatus("Unimplemented", 1, true);
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequest, Handle)
            hFunc(TEvAccessService::TEvAuthorizeRequest, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }
};

IActor* CreateMockAccessServiceWithCache() {
    return new TAccessServiceMock();
}

}
