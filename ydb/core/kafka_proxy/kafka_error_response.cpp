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

} // namespace

TApiMessage::TPtr BuildErrorResponse(const TApiMessage& request, EKafkaErrors error) {
    switch (request.ApiKey()) {
        case PRODUCE:
            return ProduceError(static_cast<const TProduceRequestData&>(request), error);
        case FETCH:
            return FetchError(static_cast<const TFetchRequestData&>(request), error);
        default:
            return nullptr;
    }
}

} // namespace NKafka
