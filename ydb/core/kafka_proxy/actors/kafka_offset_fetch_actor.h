#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

struct TopicEntities {
    std::shared_ptr<TSet<TString>> Consumers = std::make_shared<TSet<TString>>();
    std::shared_ptr<TSet<ui32>> Partitions = std::make_shared<TSet<ui32>>();
};

class TKafkaOffsetFetchActor: public NActors::TActorBootstrapped<TKafkaOffsetFetchActor> {
public:
    TKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvCommitedOffsetsResponse, Handle);
        }
    }

    void Handle(TEvKafka::TEvCommitedOffsetsResponse::TPtr& ev, const TActorContext& ctx);
    void ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic);
    TOffsetFetchResponseData::TPtr GetOffsetFetchResponse();
    void ReplyError(const TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TOffsetFetchRequestData> Message;
    std::unordered_map<TString, TopicEntities> TopicToEntities;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvCommitedOffsetsResponse>> TopicsToResponses;
    ui32 InflyTopics = 0;

};

} // NKafka
