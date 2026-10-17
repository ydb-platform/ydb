#include "actors.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

class TAlterTopicActor: public TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest>;

public:
    TAlterTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvAlterTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TAlterTopicActor::StateWork);

        // The schema actor owns the rewritten copy; the inbound request stays intact.
        auto request = *GetProtoRequest();
        request.set_path(NormalizeTopicPath(request.path()));
        for (auto& consumer : *request.mutable_add_consumers()) {
            if (consumer.shared_consumer_type().dead_letter_policy().has_move_action()) {
                auto* moveAction = consumer.mutable_shared_consumer_type()->mutable_dead_letter_policy()->mutable_move_action();
                moveAction->set_dead_letter_queue(NormalizeTopicPath(moveAction->dead_letter_queue()));
            }
        }
        for (auto& consumer : *request.mutable_alter_consumers()) {
            if (consumer.alter_shared_consumer_type().has_alter_dead_letter_policy()) {
                auto* policy = consumer.mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
                if (policy->has_set_move_action()) {
                    auto* moveAction = policy->mutable_set_move_action();
                    moveAction->set_dead_letter_queue(NormalizeTopicPath(moveAction->dead_letter_queue()));
                } else if (policy->alter_move_action().has_set_dead_letter_queue()) {
                    auto* moveAction = policy->mutable_alter_move_action();
                    moveAction->set_set_dead_letter_queue(NormalizeTopicPath(moveAction->set_dead_letter_queue()));
                }
            }
        }
        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
            .Database = GetDatabase(),
            .PeerName = Request_->GetPeerName(),
            .Request = std::move(request),
            .UserToken = GetUserToken()
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::AlterTopicResponse());
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
    
NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TAlterTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
