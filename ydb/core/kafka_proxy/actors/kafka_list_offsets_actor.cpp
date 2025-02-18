#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

#include "actors.h"
#include "kafka_list_offsets_actor.h"
#include "kafka_topic_offsets_actor.h"


namespace NKafka {

using namespace NKikimr::NGRpcProxy::V1;

using TListOffsetsPartitionResponse = TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse;


NActors::IActor* CreateKafkaListOffsetsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListOffsetsRequestData>& message) {
    return new TKafkaListOffsetsActor(context, correlationId, message);
}

void TKafkaListOffsetsActor::Bootstrap(const NActors::TActorContext& ctx) {
    ListOffsetsResponseData->Topics.resize(ListOffsetsRequestData->Topics.size());

    SendOffsetsRequests(ctx);
    Become(&TKafkaListOffsetsActor::StateWork);
    RespondIfRequired(ctx);
}

void TKafkaListOffsetsActor::SendOffsetsRequests(const NActors::TActorContext& ctx) {
    for (size_t i = 0; i < ListOffsetsRequestData->Topics.size(); ++i) {
        auto &requestTopic = ListOffsetsRequestData->Topics[i];
        auto &responseTopic = ListOffsetsResponseData->Topics[i];
        
        responseTopic = TListOffsetsResponseData::TListOffsetsTopicResponse{};
        
        if (!requestTopic.Name.has_value()) {
            HandleMissingTopicName(requestTopic, responseTopic);
            continue;
        }

        responseTopic.Name = requestTopic.Name;
        TTopicRequestInfo topicRequestInfo;
        topicRequestInfo.TopicIndex = i;
        
        for (auto& partition: requestTopic.Partitions) {
            topicRequestInfo.Partitions.push_back(TPartitionRequestInfo{.PartitionId = partition.PartitionIndex, .Timestamp = partition.Timestamp});
        }

        TopicsRequestsInfo[SendOffsetsRequest(requestTopic, ctx)] = topicRequestInfo;
    }
}

void TKafkaListOffsetsActor::HandleMissingTopicName(const TListOffsetsRequestData::TListOffsetsTopic& requestTopic, TListOffsetsResponseData::TListOffsetsTopicResponse& responseTopic) {
    for (auto& partition: requestTopic.Partitions) {
        TListOffsetsPartitionResponse responsePartition;
        responsePartition.PartitionIndex = partition.PartitionIndex;
        responsePartition.ErrorCode = INVALID_TOPIC_EXCEPTION;
        ErrorCode = INVALID_TOPIC_EXCEPTION;
        responseTopic.Partitions.emplace_back(std::move(responsePartition));
    }
}

TActorId TKafkaListOffsetsActor::SendOffsetsRequest(const TListOffsetsRequestData::TListOffsetsTopic& topic, const NActors::TActorContext&) {
    KAFKA_LOG_D("ListOffsets actor: Get offsets for topic '" << topic.Name << "' for user " << GetUsernameOrAnonymous(Context));

    TEvKafka::TGetOffsetsRequest offsetsRequest;
    offsetsRequest.Topic = NormalizePath(Context->DatabasePath, topic.Name.value());
    offsetsRequest.Token = GetUserSerializedToken(Context);
    offsetsRequest.Database = Context->DatabasePath;

    for (const auto& partitionRequest: topic.Partitions) {
        offsetsRequest.PartitionIds.push_back(partitionRequest.PartitionIndex);
    }

    PendingResponses++;
    return Register(new TTopicOffsetsActor(offsetsRequest, SelfId()));
}

void TKafkaListOffsetsActor::Handle(TEvKafka::TEvTopicOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
    --PendingResponses;
    auto it = TopicsRequestsInfo.find(ev->Sender);

    Y_DEBUG_ABORT_UNLESS(it != TopicsRequestsInfo.end()); 
    if (it == TopicsRequestsInfo.end()) {
        KAFKA_LOG_CRIT("ListOffsets actor: received unexpected TEvTopicOffsetsResponse. Ignoring.");
        return RespondIfRequired(ctx);
    }

    const auto& topicRequestInfo = it->second;
    auto& responseTopic = ListOffsetsResponseData->Topics[topicRequestInfo.TopicIndex];

    std::unordered_map<ui64, TEvKafka::TPartitionOffsetsInfo> responseFromPQPartitionsMap;
    for (size_t i = 0; i < ev->Get()->Partitions.size(); ++i) {
        responseFromPQPartitionsMap[ev->Get()->Partitions[i].PartitionId] = ev->Get()->Partitions[i];
    }

    for (auto& partitionRequestInfo: topicRequestInfo.Partitions) {
        TListOffsetsPartitionResponse responsePartition {};
        responsePartition.PartitionIndex = partitionRequestInfo.PartitionId;

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            auto it = responseFromPQPartitionsMap.find(partitionRequestInfo.PartitionId);
            if (it == responseFromPQPartitionsMap.end()) {
                KAFKA_LOG_CRIT("ListOffsets actor: partition not found. Expect malformed/incompled reply");
                continue;
            }
            auto& responseFromPQPartition = it->second;
            responsePartition.LeaderEpoch = responseFromPQPartition.Generation;
            responsePartition.Timestamp = TIMESTAMP_DEFAULT_RESPONSE_VALUE;
            
            if (partitionRequestInfo.Timestamp == TIMESTAMP_START_OFFSET) {
                responsePartition.Offset = responseFromPQPartition.StartOffset;
                responsePartition.ErrorCode = NONE_ERROR;
            } else if (partitionRequestInfo.Timestamp == TIMESTAMP_END_OFFSET) {
                responsePartition.Offset = responseFromPQPartition.EndOffset;
                responsePartition.ErrorCode = NONE_ERROR;
            } else {
                responsePartition.ErrorCode = INVALID_REQUEST; // FIXME(savnik): handle it
                ErrorCode = INVALID_REQUEST;
            }
        } else {
            responsePartition.ErrorCode = ConvertErrorCode(ev->Get()->Status);
        }

        responseTopic.Partitions.emplace_back(std::move(responsePartition));
    }

    RespondIfRequired(ctx);
}

void TKafkaListOffsetsActor::RespondIfRequired(const TActorContext& ctx) {
    if (PendingResponses == 0) {
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, ListOffsetsResponseData, ErrorCode));
        Die(ctx);
    }
}

} // namespace NKafka
