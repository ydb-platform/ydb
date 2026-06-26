#include "actors.h"
#include "common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/persqueue/public/schema/alter_topic_operation.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

struct TAlterTopicStrategy: public NPQ::NSchema::IAlterTopicStrategy {
    TAlterTopicStrategy(const Ydb::PersQueue::V1::AlterTopicRequest& request, TString&& database)
        : Request(request)
        , Database(std::move(database))
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    NPQ::NSchema::TResult ApplyChanges(
        const TString& localCluster,
        const NPQ::NDescriber::TTopicInfo& topicInfo,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig
    ) override {
        if (topicInfo.CdcStream) {
            return {Ydb::StatusIds::SCHEME_ERROR, "Full alter of CDC stream is forbidden"};
        }

        const auto& pqConfig = AppData()->PQConfig;
        const auto& sourceTabletConfig = sourceConfig.GetPQTabletConfig();

        absl::flat_hash_map<TString, std::pair<ui64, TString>> oldConsumerInfoByName;
        for (const auto& c : sourceTabletConfig.GetConsumers()) {
            oldConsumerInfoByName[c.GetName()] = {
                c.GetModificationVersion(),
                NPQ::GetDLQTopicPath(c),
            };
        }

        for (const auto& readRule : Request.settings().read_rules()) {
            const auto consumerName = NPersQueue::ConvertNewConsumerName(readRule.consumer_name(), pqConfig);
            for (const auto& existingConsumer : sourceTabletConfig.GetConsumers()) {
                if (existingConsumer.GetName() != consumerName) {
                    continue;
                }
                const bool newIsShared = readRule.has_shared_consumer_type();
                const bool oldIsShared = existingConsumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP;
                if (newIsShared != oldIsShared) {
                    return {Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "Cannot alter consumer type for consumer '" << consumerName << "'"};
                }
            }
        }

        auto result = ApplyChangesInt(Database, sourceConfig.GetName(), Request, modifyScheme, targetConfig, localCluster);
        if (!result) {
            return result;
        }

        auto& targetTabletConfig = *targetConfig.MutablePQTabletConfig();
        targetTabletConfig.SetTopicConfigVersion(sourceTabletConfig.GetTopicConfigVersion() + 1);
        for (auto& consumer : *targetTabletConfig.MutableConsumers()) {
            const TString newDlqTopicPath = NPQ::GetDLQTopicPath(consumer);
            if (const auto it = oldConsumerInfoByName.find(consumer.GetName()); it != oldConsumerInfoByName.end()) {
                if (newDlqTopicPath != it->second.second) {
                    NPQ::NSchema::UpdateConsumerVersion(consumer, targetTabletConfig);
                } else {
                    consumer.SetModificationVersion(it->second.first);
                }
            } else {
                NPQ::NSchema::UpdateConsumerVersion(consumer, targetTabletConfig);
            }
        }

        targetTabletConfig.SetLocalDC(sourceTabletConfig.GetLocalDC());
        targetTabletConfig.SetDC(sourceTabletConfig.GetDC());
        targetTabletConfig.SetProducer(sourceTabletConfig.GetProducer());
        targetTabletConfig.SetTopic(sourceTabletConfig.GetTopic());

        return {};
    }

    const Ydb::PersQueue::V1::AlterTopicRequest Request;
    const TString Database;
};

class TAlterTopicActor: public TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvPQAlterTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TAlterTopicActor, NGRpcService::TEvPQAlterTopicRequest>;

public:
    TAlterTopicActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TAlterTopicActor, NGRpcService::TEvPQAlterTopicRequest>(request)
    {
    }

    void DoAction() {
        Become(&TAlterTopicActor::StateWork);

        auto database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        Register(NPQ::NSchema::CreateAlterTopicOperationActor(SelfId(), {
            .Database = database,
            .PeerName = Request_->GetPeerName(),
            .UserToken = GetUserToken(),
            .Strategy = std::make_unique<TAlterTopicStrategy>(*GetProtoRequest(), std::move(database)),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::PersQueue::V1::AlterTopicResponse());
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

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
