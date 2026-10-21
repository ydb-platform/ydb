#include "mock_access_service.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/ycloud/api/access_service.h>

#include <memory>

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

    void Handle(TEvAccessService::TEvBulkAuthorizeRequest::TPtr& ev) {
        auto result = std::make_unique<TEvAccessService::TEvBulkAuthorizeResponse>();
        result->Status = NYdbGrpc::TGrpcStatus("Unimplemented", 1, true);
        Send(ev->Sender, result.release());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequest, Handle)
            hFunc(TEvAccessService::TEvAuthorizeRequest, Handle)
            hFunc(TEvAccessService::TEvBulkAuthorizeRequest, Handle)
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway)
        }
    }
};

IActor* CreateMockAccessServiceWithCache() {
    return new TAccessServiceMock();
}

}
