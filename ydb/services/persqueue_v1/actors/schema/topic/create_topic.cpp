#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TCreateTopicActor: public TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest>;

public:
    TCreateTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvCreateTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TCreateTopicActor::StateWork);

        // The schema actor owns the rewritten copy; the inbound request stays intact.
        auto request = *GetProtoRequest();
        request.set_path(NormalizeTopicPath(request.path()));
        for (auto& consumer : *request.mutable_consumers()) {
            if (consumer.shared_consumer_type().dead_letter_policy().has_move_action()) {
                auto* moveAction = consumer.mutable_shared_consumer_type()->mutable_dead_letter_policy()->mutable_move_action();
                moveAction->set_dead_letter_queue(NormalizeTopicPath(moveAction->dead_letter_queue()));
            }
        }
        Register(NPQ::NSchema::CreateCreateTopicActor(SelfId(), {
            .Database = GetDatabase(),
            .PeerName = Request_->GetPeerName(),
            .Request = std::move(request),
            .UserToken = GetUserToken(),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::CreateTopicResponse());
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
    
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TCreateTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
