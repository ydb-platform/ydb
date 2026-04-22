#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TDropTopicActor: public TGrpcProxyActor<TDropTopicActor, NGRpcService::TEvDropTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TDropTopicActor, NGRpcService::TEvDropTopicRequest>;

public:
    TDropTopicActor(NGRpcService::TEvDropTopicRequest* request)
        : TGrpcProxyActor<TDropTopicActor, NGRpcService::TEvDropTopicRequest>(request)
    {
    }

    TDropTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TDropTopicActor, NGRpcService::TEvDropTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TDropTopicActor::StateWork);

        Register(NPQ::NSchema::CreateDropTopicActor(SelfId(), {
            .Database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse("")),
            .PeerName = Request_->GetPeerName(),
            .Path = GetProtoRequest()->path(),
            .UserToken = GetUserToken()
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvDropTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::DropTopicResponse());
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvDropTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};

} // namespace

NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TDropTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
