#include "kafka_error_response.h"
#include "kafka_messages.h"

namespace NKafka {
namespace {

template <class TResponse>
TApiMessage::TPtr TopLevelError(EKafkaErrors error) {
    auto response = std::make_shared<TResponse>();
    response->ErrorCode = error;
    return response;
}

TApiMessage::TPtr ProduceError(const TProduceRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TProduceResponseData>();
    response->Responses.resize(request.TopicData.size());
    for (size_t i = 0; i < request.TopicData.size(); ++i) {
        auto& topicResponse = response->Responses[i];
        topicResponse.Name = request.TopicData[i].Name;
        topicResponse.PartitionResponses.resize(request.TopicData[i].PartitionData.size());
        for (size_t j = 0; j < request.TopicData[i].PartitionData.size(); ++j) {
            topicResponse.PartitionResponses[j].Index = request.TopicData[i].PartitionData[j].Index;
            topicResponse.PartitionResponses[j].ErrorCode = error;
        }
    }
    return response;
}

TApiMessage::TPtr FetchError(const TFetchRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TFetchResponseData>();
    response->ErrorCode = error;
    response->Responses.resize(request.Topics.size());
    for (size_t i = 0; i < request.Topics.size(); ++i) {
        auto& topicResponse = response->Responses[i];
        topicResponse.Topic = request.Topics[i].Topic;
        topicResponse.Partitions.resize(request.Topics[i].Partitions.size());
        for (size_t j = 0; j < request.Topics[i].Partitions.size(); ++j) {
            topicResponse.Partitions[j].PartitionIndex = request.Topics[i].Partitions[j].Partition;
            topicResponse.Partitions[j].ErrorCode = error;
        }
    }
    return response;
}

TApiMessage::TPtr ListOffsetsError(const TListOffsetsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TListOffsetsResponseData>();
    response->Topics.resize(request.Topics.size());
    for (size_t i = 0; i < request.Topics.size(); ++i) {
        auto& topicResponse = response->Topics[i];
        topicResponse.Name = request.Topics[i].Name;
        for (const auto& partition : request.Topics[i].Partitions) {
            TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse partitionResponse;
            partitionResponse.PartitionIndex = partition.PartitionIndex;
            partitionResponse.ErrorCode = error;
            topicResponse.Partitions.push_back(std::move(partitionResponse));
        }
    }
    return response;
}

TApiMessage::TPtr MetadataError(const TMetadataRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TMetadataResponseData>();
    response->Topics.resize(request.Topics.size());
    for (size_t i = 0; i < request.Topics.size(); ++i) {
        response->Topics[i].Name = request.Topics[i].Name;
        response->Topics[i].ErrorCode = error;
    }
    return response;
}

TApiMessage::TPtr OffsetCommitError(const TOffsetCommitRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TOffsetCommitResponseData>();
    for (const auto& topic : request.Topics) {
        TOffsetCommitResponseData::TOffsetCommitResponseTopic topicResponse;
        topicResponse.Name = topic.Name;
        for (const auto& partition : topic.Partitions) {
            TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition partitionResponse;
            partitionResponse.PartitionIndex = partition.PartitionIndex;
            partitionResponse.ErrorCode = error;
            topicResponse.Partitions.push_back(partitionResponse);
        }
        response->Topics.push_back(std::move(topicResponse));
    }
    return response;
}

TApiMessage::TPtr OffsetFetchError(const TOffsetFetchRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TOffsetFetchResponseData>();
    response->ErrorCode = error;

    auto appendGroup = [&](const TOffsetFetchRequestData::TOffsetFetchRequestGroup& group) {
        TOffsetFetchResponseData::TOffsetFetchResponseGroup groupResponse;
        groupResponse.GroupId = group.GroupId;
        groupResponse.ErrorCode = error;
        for (const auto& topic : group.Topics) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics topicResponse;
            topicResponse.Name = topic.Name;
            for (auto partitionIndex : topic.PartitionIndexes) {
                TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                partition.PartitionIndex = partitionIndex;
                partition.ErrorCode = error;
                topicResponse.Partitions.push_back(partition);
            }
            groupResponse.Topics.push_back(std::move(topicResponse));
        }
        response->Groups.push_back(std::move(groupResponse));
    };

    if (!request.Groups.empty()) {
        for (const auto& group : request.Groups) {
            appendGroup(group);
        }
        return response;
    }

    TOffsetFetchRequestData::TOffsetFetchRequestGroup group;
    group.GroupId = request.GroupId;
    for (const auto& topic : request.Topics) {
        TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics topicRequest;
        topicRequest.Name = topic.Name;
        topicRequest.PartitionIndexes = topic.PartitionIndexes;
        group.Topics.push_back(std::move(topicRequest));
    }
    appendGroup(group);
    return response;
}

TApiMessage::TPtr FindCoordinatorError(const TFindCoordinatorRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TFindCoordinatorResponseData>();
    response->ErrorCode = error;
    for (const auto& key : request.CoordinatorKeys) {
        TFindCoordinatorResponseData::TCoordinator coordinator;
        coordinator.ErrorCode = error;
        coordinator.Key = key;
        response->Coordinators.push_back(std::move(coordinator));
    }
    return response;
}

TApiMessage::TPtr SyncGroupError(EKafkaErrors error) {
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ErrorCode = error;
    // Non-nullable on the wire; a default (null) value fails serialization.
    response->Assignment = "";
    return response;
}

TApiMessage::TPtr DescribeGroupsError(const TDescribeGroupsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TDescribeGroupsResponseData>();
    for (const auto& groupId : request.Groups) {
        TDescribeGroupsResponseData::TDescribedGroup group;
        group.ErrorCode = error;
        group.GroupId = groupId;
        response->Groups.push_back(std::move(group));
    }
    return response;
}

TApiMessage::TPtr CreateTopicsError(const TCreateTopicsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TCreateTopicsResponseData>();
    for (const auto& topic : request.Topics) {
        TCreateTopicsResponseData::TCreatableTopicResult topicResponse;
        topicResponse.Name = topic.Name;
        topicResponse.ErrorCode = error;
        response->Topics.push_back(std::move(topicResponse));
    }
    return response;
}

TApiMessage::TPtr CreatePartitionsError(const TCreatePartitionsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TCreatePartitionsResponseData>();
    for (const auto& topic : request.Topics) {
        TCreatePartitionsResponseData::TCreatePartitionsTopicResult topicResponse;
        topicResponse.Name = topic.Name;
        topicResponse.ErrorCode = error;
        response->Results.push_back(std::move(topicResponse));
    }
    return response;
}

TApiMessage::TPtr DescribeConfigsError(const TDescribeConfigsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TDescribeConfigsResponseData>();
    for (const auto& resource : request.Resources) {
        TDescribeConfigsResponseData::TDescribeConfigsResult result;
        result.ResourceType = resource.ResourceType;
        result.ResourceName = resource.ResourceName;
        result.ErrorCode = error;
        result.ErrorMessage = "token is invalid or unavailable";
        response->Results.push_back(std::move(result));
    }
    return response;
}

TApiMessage::TPtr AlterConfigsError(const TAlterConfigsRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TAlterConfigsResponseData>();
    for (const auto& resource : request.Resources) {
        TAlterConfigsResponseData::TAlterConfigsResourceResponse resourceResponse;
        resourceResponse.ResourceName = resource.ResourceName;
        resourceResponse.ErrorCode = error;
        resourceResponse.ErrorMessage = "token is invalid or unavailable";
        response->Responses.push_back(std::move(resourceResponse));
    }
    return response;
}

TApiMessage::TPtr AddPartitionsToTxnError(const TAddPartitionsToTxnRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TAddPartitionsToTxnResponseData>();
    for (const auto& topic : request.Topics) {
        TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult topicResponse;
        topicResponse.Name = topic.Name;
        for (const auto& partition : topic.Partitions) {
            TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult partitionResponse;
            partitionResponse.PartitionIndex = partition;
            partitionResponse.ErrorCode = error;
            topicResponse.Results.push_back(partitionResponse);
        }
        response->Results.push_back(std::move(topicResponse));
    }
    return response;
}

TApiMessage::TPtr TxnOffsetCommitError(const TTxnOffsetCommitRequestData& request, EKafkaErrors error) {
    auto response = std::make_shared<TTxnOffsetCommitResponseData>();
    for (const auto& topic : request.Topics) {
        TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic topicResponse;
        topicResponse.Name = topic.Name;
        for (const auto& partition : topic.Partitions) {
            TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition partitionResponse;
            partitionResponse.PartitionIndex = partition.PartitionIndex;
            partitionResponse.ErrorCode = error;
            topicResponse.Partitions.push_back(partitionResponse);
        }
        response->Topics.push_back(std::move(topicResponse));
    }
    return response;
}

} // namespace

TApiMessage::TPtr BuildErrorResponse(const TApiMessage& request, EKafkaErrors error) {
    switch (request.ApiKey()) {
        case PRODUCE:
            return ProduceError(static_cast<const TProduceRequestData&>(request), error);
        case FETCH:
            return FetchError(static_cast<const TFetchRequestData&>(request), error);
        case LIST_OFFSETS:
            return ListOffsetsError(static_cast<const TListOffsetsRequestData&>(request), error);
        case METADATA:
            return MetadataError(static_cast<const TMetadataRequestData&>(request), error);
        case OFFSET_COMMIT:
            return OffsetCommitError(static_cast<const TOffsetCommitRequestData&>(request), error);
        case OFFSET_FETCH:
            return OffsetFetchError(static_cast<const TOffsetFetchRequestData&>(request), error);
        case FIND_COORDINATOR:
            return FindCoordinatorError(static_cast<const TFindCoordinatorRequestData&>(request), error);
        case JOIN_GROUP:
            return TopLevelError<TJoinGroupResponseData>(error);
        case HEARTBEAT:
            return TopLevelError<THeartbeatResponseData>(error);
        case LEAVE_GROUP:
            return TopLevelError<TLeaveGroupResponseData>(error);
        case SYNC_GROUP:
            return SyncGroupError(error);
        case DESCRIBE_GROUPS:
            return DescribeGroupsError(static_cast<const TDescribeGroupsRequestData&>(request), error);
        case LIST_GROUPS:
            return TopLevelError<TListGroupsResponseData>(error);
        case CREATE_TOPICS:
            return CreateTopicsError(static_cast<const TCreateTopicsRequestData&>(request), error);
        case INIT_PRODUCER_ID:
            return TopLevelError<TInitProducerIdResponseData>(error);
        case ADD_PARTITIONS_TO_TXN:
            return AddPartitionsToTxnError(static_cast<const TAddPartitionsToTxnRequestData&>(request), error);
        case ADD_OFFSETS_TO_TXN:
            return TopLevelError<TAddOffsetsToTxnResponseData>(error);
        case END_TXN:
            return TopLevelError<TEndTxnResponseData>(error);
        case TXN_OFFSET_COMMIT:
            return TxnOffsetCommitError(static_cast<const TTxnOffsetCommitRequestData&>(request), error);
        case DESCRIBE_CONFIGS:
            return DescribeConfigsError(static_cast<const TDescribeConfigsRequestData&>(request), error);
        case ALTER_CONFIGS:
            return AlterConfigsError(static_cast<const TAlterConfigsRequestData&>(request), error);
        case CREATE_PARTITIONS:
            return CreatePartitionsError(static_cast<const TCreatePartitionsRequestData&>(request), error);
        default:
            return nullptr;
    }
}

} // namespace NKafka
