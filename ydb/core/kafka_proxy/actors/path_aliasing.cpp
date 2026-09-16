#include "actors.h"
#include "kafka_read_session_utils.h"

#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKafka {

    TString ResolveKafkaRequestPaths(const TApiMessage& request, const TContext& context,
                                     std::shared_ptr<const TResolvedKafkaTopics>& resolvedTopics) {
        if (!context.PathContext || context.PathContext->Empty()) {
            return {};
        }
        std::shared_ptr<TResolvedKafkaTopics> paths;
        TString error;
        const bool transactionPaths = request.ApiKey() == ADD_PARTITIONS_TO_TXN || request.ApiKey() == TXN_OFFSET_COMMIT;
        auto add = [&](const TKafkaString& name) {
            // Existing owners retain their validation of missing and empty names.
            if (!error.empty() || !name || name->empty() || (paths && paths->contains(*name))) {
                return;
            }
            // Transaction owners use GetFullTopicPath, whose absolute-path
            // semantics differ from the other Kafka topic APIs.
            auto normalized = transactionPaths
                                  ? NKikimr::NGRpcService::ResolveConvertedTopicSchemaPath(*context.PathContext, *name,
                                                                                           NPersQueue::GetFullTopicPath(context.LogicalDatabasePath, *name))
                                  : NKikimr::NGRpcService::ResolveTopicSchemaPath(*context.PathContext,
                                                                                  context.LogicalDatabasePath, context.DatabasePath, *name);
            if (normalized.IsFail()) {
                error = normalized.GetErrorMessage();
            } else {
                if (transactionPaths && (normalized->Outcome == NKikimr::NPathAliasing::EPathRewriteOutcome::Rewritten || context.LogicalDatabasePath != context.DatabasePath) && NKikimr::CanonizePath(NPersQueue::GetFullTopicPath(context.DatabasePath, normalized->Path)) != NKikimr::CanonizePath(normalized->Path)) {
                    error = "Rewritten topic path is changed by the owner's path resolution";
                    return;
                }
                if (!paths) {
                    paths = std::make_shared<TResolvedKafkaTopics>();
                }
                paths->emplace(*name, std::move(normalized.DetachResult().Path));
            }
        };
        switch (request.ApiKey()) {
            case PRODUCE:
                for (const auto& topic : static_cast<const TProduceRequestData&>(request).TopicData) {
                    add(topic.Name);
                }
                break;
            case FETCH:
                for (const auto& topic : static_cast<const TFetchRequestData&>(request).Topics) {
                    add(topic.Topic);
                }
                break;
            case LIST_OFFSETS:
                for (const auto& topic : static_cast<const TListOffsetsRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case METADATA:
                for (const auto& topic : static_cast<const TMetadataRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case OFFSET_COMMIT:
                for (const auto& topic : static_cast<const TOffsetCommitRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case OFFSET_FETCH: {
                const auto& offsets = static_cast<const TOffsetFetchRequestData&>(request);
                for (const auto& topic : offsets.Topics) {
                    add(topic.Name);
                }
                for (const auto& group : offsets.Groups) {
                    for (const auto& topic : group.Topics) {
                        add(topic.Name);
                    }
                }
                break;
            }
            case JOIN_GROUP: {
                const auto& join = static_cast<const TJoinGroupRequestData&>(request);
                const auto mode = context.ReadSession.PendingBalancingMode
                                      ? *context.ReadSession.PendingBalancingMode
                                      : GetBalancingMode(join);
                if (mode == EBalancingMode::Native) {
                    return {}; // Native balancing metadata remains opaque Kafka data.
                }
                if (auto subscriptions = GetSubscriptions(join)) {
                    for (const auto& topic : subscriptions->Topics) {
                        add(topic);
                    }
                }
                break;
            }
            case CREATE_TOPICS:
                if (static_cast<const TCreateTopicsRequestData&>(request).ValidateOnly) {
                    return {};
                }
                for (const auto& topic : static_cast<const TCreateTopicsRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case CREATE_PARTITIONS:
                if (static_cast<const TCreatePartitionsRequestData&>(request).ValidateOnly) {
                    return {};
                }
                for (const auto& topic : static_cast<const TCreatePartitionsRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case DESCRIBE_CONFIGS:
                for (const auto& resource : static_cast<const TDescribeConfigsRequestData&>(request).Resources) {
                    if (resource.ResourceType == TOPIC_RESOURCE_TYPE) {
                        add(resource.ResourceName);
                    }
                }
                break;
            case ALTER_CONFIGS:
                if (static_cast<const TAlterConfigsRequestData&>(request).ValidateOnly) {
                    return {};
                }
                for (const auto& resource : static_cast<const TAlterConfigsRequestData&>(request).Resources) {
                    if (resource.ResourceType == TOPIC_RESOURCE_TYPE) {
                        add(resource.ResourceName);
                    }
                }
                break;
            case ADD_PARTITIONS_TO_TXN:
                for (const auto& topic : static_cast<const TAddPartitionsToTxnRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            case TXN_OFFSET_COMMIT:
                for (const auto& topic : static_cast<const TTxnOffsetCommitRequestData&>(request).Topics) {
                    add(topic.Name);
                }
                break;
            default:
                return {};
        }
        if (error.empty()) {
            if (!paths && request.ApiKey() == OFFSET_FETCH) {
                // Retain the active context for server-loaded assignment identities.
                paths = std::make_shared<TResolvedKafkaTopics>();
            }
            resolvedTopics = std::move(paths);
        }
        return error;
    }

} // namespace NKafka
