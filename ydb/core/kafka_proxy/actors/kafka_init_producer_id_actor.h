#include "../kafka_events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaInitProducerIdActor: public NActors::TActorBootstrapped<TKafkaInitProducerIdActor> {
public:
    TKafkaInitProducerIdActor(const TActorId parent, const ui64 correlationId, const TInitProducerIdRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    const TActorId Parent;
    const ui64 CorrelationId;
    const TInitProducerIdRequestData* Message;
};

} // NKafka
