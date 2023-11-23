#include "kafka_api_versions_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {

template<class T>
struct TApiVersionParams {
    TApiVersionsResponseData::TApiVersion::MinVersionMeta::Type MinVersion = T::MessageMeta::PresentVersions.Min;
    TApiVersionsResponseData::TApiVersion::MaxVersionMeta::Type MaxVersion = T::MessageMeta::PresentVersions.Max;
};

template<class T>
void AddApiKey(TApiVersionsResponseData::ApiKeysMeta::Type& apiKeys,
               const TApiVersionsResponseData::TApiVersion::ApiKeyMeta::Type apiKey,
               const TApiVersionParams<T> versions = {})
{
    auto& back = apiKeys.emplace_back();

    back.ApiKey = apiKey;
    back.MinVersion = versions.MinVersion;
    back.MaxVersion = versions.MaxVersion;
}

NActors::IActor* CreateKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TApiVersionsRequestData>& message) {
    return new TKafkaApiVersionsActor(context, correlationId, message);
}

TApiVersionsResponseData::TPtr GetApiVersions() {
    TApiVersionsResponseData::TPtr response = std::make_shared<TApiVersionsResponseData>();
    response->ErrorCode = EKafkaErrors::NONE_ERROR;

    AddApiKey<TProduceRequestData>(response->ApiKeys, PRODUCE, {.MinVersion=3});
    AddApiKey<TApiVersionsRequestData>(response->ApiKeys, API_VERSIONS);
    AddApiKey<TMetadataRequestData>(response->ApiKeys, METADATA);
    AddApiKey<TInitProducerIdRequestData>(response->ApiKeys, INIT_PRODUCER_ID);
    AddApiKey<TSaslHandshakeRequestData>(response->ApiKeys, SASL_HANDSHAKE);
    AddApiKey<TSaslAuthenticateRequestData>(response->ApiKeys, SASL_AUTHENTICATE);
    AddApiKey<TListOffsetsRequestData>(response->ApiKeys, LIST_OFFSETS);
    AddApiKey<TFetchRequestData>(response->ApiKeys, FETCH, {.MaxVersion=3});
    AddApiKey<TJoinGroupRequestData>(response->ApiKeys, JOIN_GROUP);
    AddApiKey<TSyncGroupRequestData>(response->ApiKeys, SYNC_GROUP);
    AddApiKey<TLeaveGroupRequestData>(response->ApiKeys, LEAVE_GROUP);
    AddApiKey<THeartbeatRequestData>(response->ApiKeys, HEARTBEAT);
    AddApiKey<TFindCoordinatorRequestData>(response->ApiKeys, FIND_COORDINATOR);
    AddApiKey<TOffsetCommitRequestData>(response->ApiKeys, OFFSET_COMMIT);
    AddApiKey<TOffsetFetchRequestData>(response->ApiKeys, OFFSET_FETCH);

    return response;
}

void TKafkaApiVersionsActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);
    auto apiVersions = GetApiVersions();
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, apiVersions, static_cast<EKafkaErrors>(apiVersions->ErrorCode)));
    Die(ctx);
}

}
