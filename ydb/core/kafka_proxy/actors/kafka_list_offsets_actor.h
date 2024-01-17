#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "ydb/library/aclib/aclib.h"
#include <ydb/services/persqueue_v1/actors/events.h>

#include "actors.h"

namespace NKafka {

struct TPartitionRequestInfo {
    i64 PartitionId;
    i64 Timestamp;
};

struct TTopicRequestInfo {
    size_t TopicIndex;
    std::vector<TPartitionRequestInfo> Partitions;
};

class TKafkaListOffsetsActor: public NActors::TActorBootstrapped<TKafkaListOffsetsActor> {

static constexpr int TIMESTAMP_START_OFFSET = -2;
static constexpr int TIMESTAMP_END_OFFSET = -1;
static constexpr int TIMESTAMP_DEFAULT_RESPONSE_VALUE = -1;

public:
    TKafkaListOffsetsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListOffsetsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , ListOffsetsRequestData(message)
        , ListOffsetsResponseData(new TListOffsetsResponseData()) {
    }

void Bootstrap(const NActors::TActorContext& ctx);

void Handle(TEvKafka::TEvTopicOffsetsResponse::TPtr& ev, const NActors::TActorContext& ctx);
void RespondIfRequired(const NActors::TActorContext& ctx);

private:
    void SendOffsetsRequests(const NActors::TActorContext& ctx);
    TActorId SendOffsetsRequest(const TListOffsetsRequestData::TListOffsetsTopic& topic, const NActors::TActorContext&);
    void HandleMissingTopicName(const TListOffsetsRequestData::TListOffsetsTopic& requestTopic, TListOffsetsResponseData::TListOffsetsTopicResponse& responseTopic);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvTopicOffsetsResponse, Handle);
        }
    }

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TListOffsetsRequestData> ListOffsetsRequestData;
    ui64 PendingResponses = 0;
    const TListOffsetsResponseData::TPtr ListOffsetsResponseData;

    EKafkaErrors ErrorCode = EKafkaErrors::NONE_ERROR;
    std::unordered_map<TActorId, TTopicRequestInfo> TopicsRequestsInfo;
};

} // namespace NKafka
