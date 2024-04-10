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

    AddApiKey<TProduceRequestData>(response->ApiKeys, PRODUCE, {.MinVersion=3, .MaxVersion=9});
    AddApiKey<TApiVersionsRequestData>(response->ApiKeys, API_VERSIONS, {.MaxVersion=2});
    AddApiKey<TMetadataRequestData>(response->ApiKeys, METADATA, {.MaxVersion=9});
    AddApiKey<TInitProducerIdRequestData>(response->ApiKeys, INIT_PRODUCER_ID, {.MaxVersion=4});
    AddApiKey<TSaslHandshakeRequestData>(response->ApiKeys, SASL_HANDSHAKE, {.MaxVersion=1});
    AddApiKey<TSaslAuthenticateRequestData>(response->ApiKeys, SASL_AUTHENTICATE, {.MaxVersion=2});
    AddApiKey<TListOffsetsRequestData>(response->ApiKeys, LIST_OFFSETS, {.MinVersion=1, .MaxVersion=1});
    AddApiKey<TFetchRequestData>(response->ApiKeys, FETCH, {.MaxVersion=3});
    AddApiKey<TJoinGroupRequestData>(response->ApiKeys, JOIN_GROUP, {.MaxVersion=9});
    AddApiKey<TSyncGroupRequestData>(response->ApiKeys, SYNC_GROUP, {.MaxVersion=3});
    AddApiKey<TLeaveGroupRequestData>(response->ApiKeys, LEAVE_GROUP, {.MaxVersion=5});
    AddApiKey<THeartbeatRequestData>(response->ApiKeys, HEARTBEAT, {.MaxVersion=4});
    AddApiKey<TFindCoordinatorRequestData>(response->ApiKeys, FIND_COORDINATOR, {.MaxVersion=0});
    AddApiKey<TOffsetCommitRequestData>(response->ApiKeys, OFFSET_COMMIT, {.MaxVersion=0});
    AddApiKey<TOffsetFetchRequestData>(response->ApiKeys, OFFSET_FETCH, {.MaxVersion=8});
    AddApiKey<TCreateTopicsRequestData>(response->ApiKeys, CREATE_TOPICS, {.MaxVersion=7});
    AddApiKey<TAlterConfigsRequestData>(response->ApiKeys, ALTER_CONFIGS, {.MaxVersion=2});
    AddApiKey<TCreatePartitionsRequestData>(response->ApiKeys, CREATE_PARTITIONS, {.MaxVersion=3});

    return response;
}

void TKafkaApiVersionsActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);
    auto apiVersions = GetApiVersions();
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, apiVersions, static_cast<EKafkaErrors>(apiVersions->ErrorCode)));
    Die(ctx);
}

}
