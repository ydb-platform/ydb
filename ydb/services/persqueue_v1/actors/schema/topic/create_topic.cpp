#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TCreateTopicActor: public TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest>;

public:
    TCreateTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localDc)
        : TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest>(request)
        , LocalDc(localDc)
    {
    }

    void DoAction() {
        Become(&TCreateTopicActor::StateWork);

        Register(NPQ::NSchema::CreateCreateTopicActor(SelfId(), {
            .Database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse("")),
            .PeerName = Request_->GetPeerName(),
            .Request = *GetProtoRequest(),
            .UserToken = GetUserToken(),
            .LocalDc = std::move(LocalDc),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::CreateTopicResponse());
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvCreateTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    TString LocalDc;
};

} // namespace
    
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localDc) {
    return new TCreateTopicActor(request, localDc);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
