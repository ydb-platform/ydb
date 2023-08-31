#include "actors.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>

namespace NKafka {

class TKafkaMetadataActor: public NActors::TActorBootstrapped<TKafkaMetadataActor> {
public:
    TKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId, const TMetadataRequestData* message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , Response(new TMetadataResponseData())
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;

    TActorId SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest);
    void HandleResponse(TEvLocationResponse::TPtr ev, const NActors::TActorContext& ctx);

    void AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response);
    void AddTopicError(TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode);
    void RespondIfRequired(const NActors::TActorContext& ctx);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocationResponse, HandleResponse);
        }
    }

    TString LogPrefix() const;

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMetadataRequestData* Message;

    ui64 PendingResponses = 0;

    TMetadataResponseData::TPtr Response;
    THashMap<TActorId, TVector<ui64>> TopicIndexes;
    THashSet<ui64> AllClusterNodes;
    EKafkaErrors ErrorCode = EKafkaErrors::NONE_ERROR;
};

} // namespace NKafka
