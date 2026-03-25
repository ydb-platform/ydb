#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/json/json_value.h>
#include "access_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>

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

    void Handle(TEvAccessService::TEvAuthenticateRequestV1::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthenticateResponseV1>();
        result->Response.mutable_subject()->mutable_user_account()->set_federation_id("mock");
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthorizeRequestV1::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthorizeResponseV1>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", grpc::StatusCode::UNIMPLEMENTED, true);
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthenticateRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthenticateResponseV2>();
        result->Response.mutable_subject()->mutable_user_account()->set_federation_id("mock");
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthorizeRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthorizeResponseV2>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", grpc::StatusCode::UNIMPLEMENTED, true);
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvBulkAuthorizeRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvBulkAuthorizeResponseV2>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", grpc::StatusCode::UNIMPLEMENTED, true);
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequestV1, Handle)
            hFunc(TEvAccessService::TEvAuthorizeRequestV1, Handle)
            hFunc(TEvAccessService::TEvAuthenticateRequestV2, Handle)
            hFunc(TEvAccessService::TEvAuthorizeRequestV2, Handle)
            hFunc(TEvAccessService::TEvBulkAuthorizeRequestV2, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }
};

IActor* CreateMockAccessServiceWithCache() {
    return new TAccessServiceMock();
}

}
