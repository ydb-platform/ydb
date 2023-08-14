#include "kafka_api_versions_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {

NActors::IActor* CreateKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TApiVersionsRequestData* message) {
    return new TKafkaApiVersionsActor(context, correlationId, message);
}    

TApiVersionsResponseData::TPtr GetApiVersions() {
    TApiVersionsResponseData::TPtr response = std::make_shared<TApiVersionsResponseData>();
    response->ApiKeys.resize(6);

    response->ApiKeys[0].ApiKey = PRODUCE;
    response->ApiKeys[0].MinVersion = 3; // From version 3 record batch format is 2. Supported only 2th batch format.
    response->ApiKeys[0].MaxVersion = TProduceRequestData::MessageMeta::PresentVersions.Max;

    response->ApiKeys[1].ApiKey = API_VERSIONS;
    response->ApiKeys[1].MinVersion = TApiVersionsRequestData::MessageMeta::PresentVersions.Min;
    response->ApiKeys[1].MaxVersion = TApiVersionsRequestData::MessageMeta::PresentVersions.Max;

    response->ApiKeys[2].ApiKey = METADATA;
    response->ApiKeys[2].MinVersion = TMetadataRequestData::MessageMeta::PresentVersions.Min;
    response->ApiKeys[2].MaxVersion = TMetadataRequestData::MessageMeta::PresentVersions.Max;

    response->ApiKeys[3].ApiKey = INIT_PRODUCER_ID;
    response->ApiKeys[3].MinVersion = TInitProducerIdRequestData::MessageMeta::PresentVersions.Min;
    response->ApiKeys[3].MaxVersion = TInitProducerIdRequestData::MessageMeta::PresentVersions.Max;

    response->ApiKeys[4].ApiKey = SASL_HANDSHAKE;
    response->ApiKeys[4].MinVersion = TSaslHandshakeRequestData::MessageMeta::PresentVersions.Min;
    response->ApiKeys[4].MaxVersion = TSaslHandshakeRequestData::MessageMeta::PresentVersions.Max;

    response->ApiKeys[5].ApiKey = SASL_AUTHENTICATE;
    response->ApiKeys[5].MinVersion = TSaslAuthenticateRequestData::MessageMeta::PresentVersions.Min;
    response->ApiKeys[5].MaxVersion = TSaslAuthenticateRequestData::MessageMeta::PresentVersions.Max;

    return response;
}

void TKafkaApiVersionsActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, GetApiVersions()));
    Die(ctx);
}

}
