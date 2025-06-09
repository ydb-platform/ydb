#include "txn_actor_response_builder.h"

namespace NKafka::NKafkaTransactions {
    template<class ResponseType, class RequestType>
    std::shared_ptr<ResponseType> BuildResponse(TMessagePtr<RequestType> request, EKafkaErrors errorCode) {
        Y_UNUSED(request); // used in other template functions
        auto response = std::make_shared<ResponseType>();
        response->ErrorCode = errorCode;
        return response;
    };
    
    template std::shared_ptr<TAddOffsetsToTxnResponseData> BuildResponse<TAddOffsetsToTxnResponseData, TAddOffsetsToTxnRequestData>(TMessagePtr<TAddOffsetsToTxnRequestData> request, EKafkaErrors errorCode);
    
    template std::shared_ptr<TEndTxnResponseData> BuildResponse<TEndTxnResponseData, TEndTxnRequestData>(TMessagePtr<TEndTxnRequestData> request, EKafkaErrors errorCode);

    template<>
    std::shared_ptr<TAddPartitionsToTxnResponseData> BuildResponse<TAddPartitionsToTxnResponseData, TAddPartitionsToTxnRequestData>(TMessagePtr<TAddPartitionsToTxnRequestData> request, EKafkaErrors errorCode) {
        auto response = std::make_shared<TAddPartitionsToTxnResponseData>();
        std::vector<TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (const auto& requestTopic : request->Topics) {
            TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Results.reserve(requestTopic.Partitions.size());
            for (const auto& requestPartition : requestTopic.Partitions) {
                TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition;
                partitionInResponse.ErrorCode = errorCode;
                topicInResponse.Results.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Results = std::move(topicsResponse);
        return response;
    };

    template<>
    std::shared_ptr<TTxnOffsetCommitResponseData> BuildResponse<TTxnOffsetCommitResponseData, TTxnOffsetCommitRequestData>(TMessagePtr<TTxnOffsetCommitRequestData> request, EKafkaErrors errorCode) {
        auto response = std::make_shared<TTxnOffsetCommitResponseData>();
        std::vector<TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (const auto& requestTopic : request->Topics) {
            TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Partitions.reserve(requestTopic.Partitions.size());
            for (const auto& requestPartition : requestTopic.Partitions) {
                TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition.PartitionIndex;
                partitionInResponse.ErrorCode = errorCode;
                topicInResponse.Partitions.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Topics = std::move(topicsResponse);;
        return response;
    };
} // namespace NKafka::NKafkaTransactions