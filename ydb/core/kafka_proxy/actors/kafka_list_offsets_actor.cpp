#include <library/cpp/actors/core/actor.h>
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
        std::unordered_map<ui64, TPartitionRequestInfo> partitionsMap; 

        for (auto& partition: requestTopic.Partitions) {
            partitionsMap[partition.PartitionIndex] = TPartitionRequestInfo{.Timestamp = partition.Timestamp};
        }

        TopicsRequestsInfo[SendOffsetsRequest(requestTopic, ctx)] = {i, partitionsMap};
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
    KAFKA_LOG_D("Get offsets for topic '" << topic.Name << "' for user '" << Context->UserToken->GetUserSID() << "'");
    
    TEvKafka::TGetOffsetsRequest offsetsRequest;
    offsetsRequest.Topic = topic.Name.value();
    offsetsRequest.Token = Context->UserToken->GetSerializedToken();
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
    if (it == TopicsRequestsInfo.end()) {
        KAFKA_LOG_CRIT("ListOffsets actor: received unexpected TEvTopicOffsetsResponse. Ignoring.");
        return RespondIfRequired(ctx);
    }

    const auto& topicIndex = it->second.first;
    auto& partitionsRequestInfoMap = it->second.second;

    auto& responseTopic = ListOffsetsResponseData->Topics[topicIndex];
    responseTopic.Partitions.reserve(ev->Get()->Partitions.size());

    for (auto& partition: ev->Get()->Partitions) {
        TListOffsetsPartitionResponse responsePartition {};
        responsePartition.PartitionIndex = partition.PartitionId;

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            responsePartition.LeaderEpoch = partition.Generation;

            auto timestamp = partitionsRequestInfoMap[partition.PartitionId].Timestamp;
            responsePartition.Timestamp = TIMESTAMP_DEFAULT_RESPONSE_VALUE;
            
            if (timestamp == TIMESTAMP_START_OFFSET) {
                responsePartition.Offset = partition.StartOffset;
                responsePartition.ErrorCode = NONE_ERROR;
            } else if (timestamp == TIMESTAMP_END_OFFSET) {
                responsePartition.Offset = partition.EndOffset;
                responsePartition.ErrorCode = NONE_ERROR;
            } else {
                responsePartition.ErrorCode = INVALID_REQUEST; //TODO savnik: handle it
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
