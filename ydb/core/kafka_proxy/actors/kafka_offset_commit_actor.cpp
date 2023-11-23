#include "kafka_offset_commit_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {


NActors::IActor* CreateKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message) {
    return new TKafkaOffsetCommitActor(context, correlationId, message);
}

TOffsetCommitResponseData::TPtr TKafkaOffsetCommitActor::GetOffsetCommitResponse() {
    TOffsetCommitResponseData::TPtr response = std::make_shared<TOffsetCommitResponseData>();

    for (auto topicReq: Message->Topics) {
        TOffsetCommitResponseData::TOffsetCommitResponseTopic topic;
        topic.Name = topicReq.Name;
        for (auto partitionRequest: topicReq.Partitions) {
            TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition partition;
            partition.PartitionIndex = partitionRequest.PartitionIndex;
            partition.ErrorCode = NONE_ERROR;
            topic.Partitions.push_back(partition);
        }
        response->Topics.push_back(topic);
    }

    return response;
}

void TKafkaOffsetCommitActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);
    auto response = GetOffsetCommitResponse();

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, EKafkaErrors::NONE_ERROR));
    Die(ctx);
}

} // NKafka
