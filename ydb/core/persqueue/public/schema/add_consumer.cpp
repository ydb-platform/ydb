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

struct TAddConsumerStrategy: public IAlterTopicStrategy {
    TAddConsumerStrategy(TString&& path, Ydb::Topic::Consumer&& consumer)
        : Path(std::move(path))
        , Consumer(std::move(consumer))
    {
    }

    const TString& GetTopicName() const override {
        return Path;
    }

    TResult ApplyChanges(
        const TString& /*localCluster*/,
        const NDescriber::TTopicInfo& /*topicInfo*/,
        NKikimrSchemeOp::TModifyScheme& /*modifyScheme*/,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig
    ) override {
        CopyConfig(targetConfig, sourceConfig);

        auto* config = targetConfig.MutablePQTabletConfig();
        return AddConsumer(config, Consumer, GetSupportedClientServiceTypes(), true, nullptr);
    }

    const TString Path;
    const Ydb::Topic::Consumer Consumer;
};

} // namespace

NActors::IActor* CreateAddConsumerActor(const NActors::TActorId& parentId, TAddConsumerSettings&& settings) {
    return CreateAlterTopicOperationActor(parentId, {
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .UserToken = std::move(settings.UserToken),
        .Strategy = std::make_unique<TAddConsumerStrategy>(std::move(settings.Path), std::move(settings.Consumer)),
        .Cookie = settings.Cookie,
    });
}

} // namespace NKikimr::NPQ::NSchema
