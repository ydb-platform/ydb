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

    void Handle(TEvAccessService::TEvAuthenticateRequest::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthenticateResponse>();
        result->Response.mutable_subject()->mutable_user_account()->set_federation_id("mock");
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthorizeRequest::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthorizeResponse>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", 1, true);
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

class TAccessServiceMockV2
    : public NActors::TActor<TAccessServiceMockV2> {
    using TThis = TAccessServiceMockV2;
    using TBase = NActors::TActor<TAccessServiceMockV2>;

public:
    TAccessServiceMockV2()
        : TBase(&TThis::StateWork) {
    }

    void Handle(TEvAccessService::TEvAuthenticateRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthenticateResponseV2>();
        result->Response.mutable_subject()->mutable_user_account()->set_federation_id("mock");
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvAuthorizeRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvAuthorizeResponseV2>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", 1, true);
        Send(ev->Sender, result.release());
    }

    void Handle(TEvAccessService::TEvBulkAuthorizeRequestV2::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvBulkAuthorizeResponseV2>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", 1, true);
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequestV2, Handle)
            hFunc(TEvAccessService::TEvAuthorizeRequestV2, Handle)
            hFunc(TEvAccessService::TEvBulkAuthorizeRequestV2, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }
};

IActor* CreateMockAccessServiceWithCache(bool enableV2Interface) {
    if (enableV2Interface) {
        return new TAccessServiceMockV2();
    }
    return new TAccessServiceMock();
}

}
