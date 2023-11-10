#include "kafka_offset_fetch_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {


NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message) {
    return new TKafkaOffsetFetchActor(context, correlationId, message);
}

TOffsetFetchResponseData::TPtr TKafkaOffsetFetchActor::GetOffsetFetchResponse() {
    TOffsetFetchResponseData::TPtr response = std::make_shared<TOffsetFetchResponseData>();

    for (auto groupReq: Message->Groups) {
        TOffsetFetchResponseData::TOffsetFetchResponseGroup group;
        group.GroupId = groupReq.GroupId;
        for (auto topicReq: groupReq.Topics) {
            TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics topic;
            topic.Name = topicReq.Name;
            for (auto part: topicReq.PartitionIndexes) {
                TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions partition;
                partition.PartitionIndex = part;
                partition.CommittedOffset = 0;
                partition.ErrorCode = NONE_ERROR;
                topic.Partitions.push_back(partition);
            }
            group.Topics.push_back(topic);
        }
        response->Groups.push_back(group);
    }
    
    return response;
}

void TKafkaOffsetFetchActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);
    auto response = GetOffsetFetchResponse();
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
    Die(ctx);
}

}
