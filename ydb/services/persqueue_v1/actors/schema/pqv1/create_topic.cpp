#include "actors.h"
#include "common.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/create_topic_operation.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

struct TCreateTopicStrategy: public NPQ::NSchema::ICreateTopicStrategy {
    TCreateTopicStrategy(const Ydb::PersQueue::V1::CreateTopicRequest& request)
        : Request(request)
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    NPQ::NSchema::TResult ApplyChanges(
        const TString& localCluster,
        const TString& database,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig
    ) override {
        return ApplyChangesInt(database, Request, modifyScheme, targetConfig, localCluster);
    }

    const Ydb::PersQueue::V1::CreateTopicRequest Request;
};

class TCreateTopicActor: public TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest>;

public:
    TCreateTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TCreateTopicActor::StateWork);

        auto database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        Register(NPQ::NSchema::CreateCreateTopicOperationActor(SelfId(), {
            .Database = database,
            .PeerName = Request_->GetPeerName(),
            .UserToken = GetUserToken(),
            .Strategy = std::make_unique<TCreateTopicStrategy>(*GetProtoRequest()),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::PersQueue::V1::CreateTopicResponse());
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvCreateTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};

} // namespace
    
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TCreateTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
