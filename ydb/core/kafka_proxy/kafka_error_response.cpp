#include "kafka_error_response.h"
#include "kafka_messages.h"

namespace NKafka {
namespace {

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

} // namespace

TApiMessage::TPtr BuildErrorResponse(const TApiMessage& request, EKafkaErrors error) {
    switch (request.ApiKey()) {
        case PRODUCE:
            return ProduceError(static_cast<const TProduceRequestData&>(request), error);
        case FETCH:
            return FetchError(static_cast<const TFetchRequestData&>(request), error);
        case LIST_OFFSETS:
            return ListOffsetsError(static_cast<const TListOffsetsRequestData&>(request), error);
        case OFFSET_FETCH:
            return OffsetFetchError(static_cast<const TOffsetFetchRequestData&>(request), error);
        default:
            return nullptr;
    }
}

} // namespace NKafka
