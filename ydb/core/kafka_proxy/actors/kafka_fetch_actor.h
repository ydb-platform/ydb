#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/aclib/aclib.h>


namespace NKafka {

class TKafkaFetchActor: public NActors::TActorBootstrapped<TKafkaFetchActor> {
public:
    TKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , FetchRequestData(message)
        , Response(new TFetchResponseData())
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    using TActorContext = NActors::TActorContext;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvPQ::TEvFetchResponse, Handle);
        }
    }

    void Handle(NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, const TActorContext& ctx);

    void SendFetchRequests(const TActorContext& ctx);
    void PrepareFetchRequestData(const size_t topicIndex, TVector<NKikimr::NPQ::TPartitionFetchRequest>& partPQRequests);
    void HandleErrorResponse(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, TFetchResponseData::TFetchableTopicResponse& topicResponse);
    void HandleSuccessResponse(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev, TFetchResponseData::TFetchableTopicResponse& topicResponse, const TActorContext& ctx);
    void FillRecordsBatch(const NKikimrClient::TPersQueueFetchResponse_TPartResult& partPQResponse, TKafkaRecordBatch& recordsBatch, const TActorContext& ctx);
    void RespondIfRequired(const TActorContext& ctx);
    size_t CheckTopicIndex(const NKikimr::TEvPQ::TEvFetchResponse::TPtr& ev);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TFetchRequestData> FetchRequestData;
    const TFetchResponseData::TPtr Response;
    std::unordered_map<TActorId, size_t> TopicIndexes;
    ui64 PendingResponses = 0;
    EKafkaErrors ErrorCode = EKafkaErrors::NONE_ERROR;
};

NActors::IActor* CreateKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message);

} // namespace NKafka
