#include "kafka_init_producer_id_actor.h"

#include <util/random/random.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {

NActors::IActor* CreateKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TInitProducerIdRequestData>& message) {
    return new TKafkaInitProducerIdActor(context, correlationId, message);
}    

TInitProducerIdResponseData::TPtr GetResponse(const NActors::TActorContext& ctx) {
    TInitProducerIdResponseData::TPtr response = std::make_shared<TInitProducerIdResponseData>();

    response->ProducerEpoch = 0;
    response->ProducerId = ((ctx.Now().MilliSeconds() << 16) & 0x7FFFFFFFFFFF) + RandomNumber<ui16>();
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->ThrottleTimeMs = 0;

    return response;
}

void TKafkaInitProducerIdActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);
    auto response = GetResponse(ctx);
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
    Die(ctx);
}

}
