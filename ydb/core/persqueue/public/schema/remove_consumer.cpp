#include "alter_topic_operation.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ::NSchema {

namespace {

struct TRemoveConsumerStrategy: public IAlterTopicStrategy {
    TRemoveConsumerStrategy(TString&& path, TString&& consumerName)
        : Path(std::move(path))
        , ConsumerName(std::move(consumerName))
    {
    }

    const TString& GetTopicName() const override {
        return Path;
    }

    TResult ApplyChanges(
        const NDescriber::TTopicInfo& /*topicInfo*/,
        NKikimrSchemeOp::TModifyScheme& /*modifyScheme*/,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig
    ) override {
        targetConfig.CopyFrom(sourceConfig);

        // keep previous values or set in ModifyPersqueueConfig
        targetConfig.ClearTotalGroupCount();

        auto* config = targetConfig.MutablePQTabletConfig();
        config->ClearPartitionKeySchema();
        config->ClearConsumers();

        bool removed = false;
        for (auto& consumer : sourceConfig.GetPQTabletConfig().GetConsumers()) {
            if (ConsumerName == consumer.GetName()) {
                removed = true;
                continue;
            }

            auto* dst = config->AddConsumers();
            dst->CopyFrom(consumer);
        }

        if (!removed) {
            return {Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Rule for consumer " << ConsumerName << " doesn't exist"};
        }

        return {};
    }

    const TString Path;
    const TString ConsumerName;
};

} // namespace

NActors::IActor* CreateRemoveConsumerActor(const NActors::TActorId& parentId, TRemoveConsumerSettings&& settings) {
    return CreateAlterTopicOperationActor(parentId, {
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .UserToken = std::move(settings.UserToken),
        .Strategy = std::make_unique<TRemoveConsumerStrategy>(std::move(settings.Path), std::move(settings.ConsumerName)),
        .Cookie = settings.Cookie,
    });
}

} // namespace NKikimr::NPQ::NSchema
