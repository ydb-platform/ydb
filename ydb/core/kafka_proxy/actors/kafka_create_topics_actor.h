#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

class TKafkaCreateTopicsActor: public NActors::TActorBootstrapped<TKafkaCreateTopicsActor> {
public:
    TKafkaCreateTopicsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TCreateTopicsRequestData>& message)
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
    const TMessagePtr<TCreateTopicsRequestData> Message;
    std::unordered_set<TString> DuplicateTopicNames;
    ui32 InflyTopics = 0;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvTopicModificationResponse>> TopicNamesToResponses;
    std::unordered_map<TString, std::pair<std::optional<ui64>, std::optional<ui64>>> TopicNamesToRetentions;

    TStringBuilder InputLogMessage();
    void ProcessValidateOnly(const NActors::TActorContext& ctx);
};

} // NKafka
