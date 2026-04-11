#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

class TAlterTopicActor : public TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest>;

public:
    TAlterTopicActor(NGRpcService::TEvAlterTopicRequest* request)
        : TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest>(request)
    {
    }

    TAlterTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TAlterTopicActor::StateWork);

        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
            .Database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse("")),
            .PeerName = Request_->GetPeerName(),
            .Request = *GetProtoRequest(),
            .UserToken = GetUserToken()
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            Ydb::Topic::AlterTopicResponse result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};
    
NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TAlterTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic