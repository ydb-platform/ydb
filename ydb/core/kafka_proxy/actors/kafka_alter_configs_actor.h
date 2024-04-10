#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaAlterConfigsActor: public NActors::TActorBootstrapped<TKafkaAlterConfigsActor> {
public:
    TKafkaAlterConfigsActor(
            const TContext::TPtr context,
            const ui64 correlationId,
            const TMessagePtr<TAlterConfigsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);
    void Handle(const TEvKafka::TEvTopicModificationResponse::TPtr& ev, const TActorContext& ctx);
    void Reply(const TActorContext& ctx);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvTopicModificationResponse, Handle);
        }
    }

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TAlterConfigsRequestData> Message;
    std::unordered_set<TString> DuplicateTopicNames;
    ui32 InflyTopics = 0;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvTopicModificationResponse>> TopicNamesToResponses;

    TStringBuilder InputLogMessage();
    void ProcessValidateOnly(const NActors::TActorContext& ctx);
};

} // NKafka
