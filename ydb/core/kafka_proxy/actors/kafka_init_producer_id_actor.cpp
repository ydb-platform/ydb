#include "kafka_init_producer_id_actor.h"

namespace NKafka {

NActors::IActor* CreateKafkaInitProducerIdActor(const TActorId parent, const ui64 correlationId, const TInitProducerIdRequestData* message) {
    return new TKafkaInitProducerIdActor(parent, correlationId, message);
}    

TInitProducerIdResponseData::TPtr GetResponse() {
    TInitProducerIdResponseData::TPtr response = std::make_shared<TInitProducerIdResponseData>();

    response->ProducerEpoch = 1;
    response->ProducerId = 1;
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->ThrottleTimeMs = 0;

    return response;
}

void TKafkaInitProducerIdActor::Bootstrap(const NActors::TActorContext& ctx) {
    Y_UNUSED(Message);

    Send(Parent, new TEvKafka::TEvResponse(CorrelationId, GetResponse()));
    Die(ctx);
}

}
