#include "actors.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaApiVersionsActor: public NActors::TActorBootstrapped<TKafkaApiVersionsActor> {
public:
    TKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TApiVersionsRequestData* message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TApiVersionsRequestData* Message;
};

} // NKafka
