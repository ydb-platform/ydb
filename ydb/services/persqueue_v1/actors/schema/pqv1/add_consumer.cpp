#include "actors.h"
#include "common.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/alter_topic_operation.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

struct TAddConsumerStrategy: public NPQ::NSchema::IAlterTopicStrategy {
    TAddConsumerStrategy(const Ydb::PersQueue::V1::TopicSettings::ReadRule& rule, const TString& path)
        : Rule(rule)
        , Path(path)
    {
    }

    const TString& GetTopicName() const override {
        return Path;
    }

    NPQ::NSchema::TResult ApplyChanges(
        const NPQ::NDescriber::TTopicInfo& topicInfo,
        NKikimrSchemeOp::TModifyScheme& /*modifyScheme*/,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig
    ) override {
        targetConfig.CopyFrom(sourceConfig);

        // keep previous values or set in ModifyPersqueueConfig
        targetConfig.ClearTotalGroupCount();
        targetConfig.MutablePQTabletConfig()->ClearPartitionKeySchema();

        if (Rule.version() == 0) {
            Rule.set_version(topicInfo.Self->Info.GetVersion().GetPQVersion());
        }

        return AddConsumer(targetConfig.MutablePQTabletConfig(), Rule, nullptr);
    }

    Ydb::PersQueue::V1::TopicSettings::ReadRule Rule;
    const TString Path;
};

class TAddConsumerActor: public TGrpcProxyActor<TAddConsumerActor, NGRpcService::TEvPQAddReadRuleRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TAddConsumerActor, NGRpcService::TEvPQAddReadRuleRequest>;

public:
    TAddConsumerActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TAddConsumerActor, NGRpcService::TEvPQAddReadRuleRequest>(request)
    {
    }

    void DoAction() {
        Become(&TAddConsumerActor::StateWork);

        auto database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        Register(NPQ::NSchema::CreateAlterTopicOperationActor(SelfId(), {
            .Database = database,
            .PeerName = Request_->GetPeerName(),
            .UserToken = GetUserToken(),
            .Strategy = std::make_unique<TAddConsumerStrategy>(GetProtoRequest()->read_rule(), GetProtoRequest()->path()),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        auto status = ev->Get()->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            this->Reply(Ydb::StatusIds::SUCCESS);
        } else {
            ReplyWithError(status, status, ev->Get()->ErrorMessage);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    TString LocalDc;
};

} // namespace
    
NActors::IActor* CreateAddConsumerActor(NGRpcService::IRequestOpCtx* request) {
    return new TAddConsumerActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
