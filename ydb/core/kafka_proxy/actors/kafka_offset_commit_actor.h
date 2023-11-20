#include "actors.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaOffsetCommitActor: public NActors::TActorBootstrapped<TKafkaOffsetCommitActor> {
public:
    TKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);
    TOffsetCommitResponseData::TPtr GetOffsetCommitResponse();

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TOffsetCommitRequestData> Message;
};

} // NKafka
