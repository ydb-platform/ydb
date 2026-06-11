#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/alter_topic_operation.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

class TRemoveConsumerActor: public TGrpcProxyActor<TRemoveConsumerActor, NGRpcService::TEvPQRemoveReadRuleRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TRemoveConsumerActor, NGRpcService::TEvPQRemoveReadRuleRequest>;

public:
    TRemoveConsumerActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TRemoveConsumerActor, NGRpcService::TEvPQRemoveReadRuleRequest>(request)
    {
    }

    void DoAction() {
        Become(&TRemoveConsumerActor::StateWork);

        Register(NPQ::NSchema::CreateRemoveConsumerActor(SelfId(), {
            .Database = this->Request_->GetDatabaseName().GetOrElse(""),
            .PeerName = Request_->GetPeerName(),
            .Path = GetProtoRequest()->path(),
            .ConsumerName = GetProtoRequest()->consumer_name(),
            .UserToken = GetUserToken(),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        auto status = ev->Get()->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::PersQueue::V1::RemoveReadRuleResponse());
        } else {
            ReplyWithError(status, ev->Get()->ErrorMessage);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvSchemaResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};

} // namespace
    
NActors::IActor* CreateRemoveConsumerActor(NGRpcService::IRequestOpCtx* request) {
    return new TRemoveConsumerActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
