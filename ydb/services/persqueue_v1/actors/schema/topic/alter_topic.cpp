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

        NPQ::NSchema::TAlterTopicSettings settings{
            .Database = GetDatabase(),
            .PeerName = Request_->GetPeerName(),
            .Request = *GetProtoRequest(),
            .UserToken = GetUserToken()
        };
        settings.Request.set_path(GetTopicPath());
        settings.PathContext = GetFederatedPathContext();
        settings.LogicalDatabase = GetLogicalDatabase();
        for (auto& consumer : *settings.Request.mutable_add_consumers()) {
            if (!ResolveConsumerSchemaReferences(consumer)) {
                return;
            }
        }
        for (auto& consumer : *settings.Request.mutable_alter_consumers()) {
            if (!consumer.has_alter_shared_consumer_type()
                || !consumer.alter_shared_consumer_type().has_alter_dead_letter_policy()) {
                continue;
            }
            auto* policy = consumer.mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
            if (policy->has_alter_move_action() && policy->alter_move_action().has_set_dead_letter_queue()) {
                if (!ResolveDeadLetterQueue(*policy->mutable_alter_move_action()->mutable_set_dead_letter_queue())) {
                    return;
                }
            } else if (policy->has_set_move_action()) {
                if (!ResolveDeadLetterQueue(*policy->mutable_set_move_action()->mutable_dead_letter_queue())) {
                    return;
                }
            }
        }
        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), std::move(settings)));
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
