#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaInitProducerIdActor: public NActors::TActorBootstrapped<TKafkaInitProducerIdActor> {
public:
    TKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TInitProducerIdRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TInitProducerIdRequestData> Message;
};

} // NKafka
