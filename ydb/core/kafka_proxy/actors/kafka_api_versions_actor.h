#include "../kafka_events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaApiVersionsActor: public NActors::TActorBootstrapped<TKafkaApiVersionsActor> {
public:
    TKafkaApiVersionsActor(const TActorId parent, const ui64 correlationId, const TApiVersionsRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    const TActorId Parent;
    const ui64 CorrelationId;
    const TApiVersionsRequestData* Message;
};

} // NKafka
