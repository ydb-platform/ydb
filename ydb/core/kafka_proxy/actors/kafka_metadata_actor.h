#include "actors.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

namespace NKafka {

class TKafkaMetadataActor: public NActors::TActorBootstrapped<TKafkaMetadataActor> {
public:
    TKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TMetadataRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , WithProxy(context->Config.HasProxy() && !context->Config.GetProxy().GetHostname().empty())
        , Response(new TMetadataResponseData())
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;

    TActorId SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest);
    void HandleResponse(TEvLocationResponse::TPtr ev, const NActors::TActorContext& ctx);
    void HandleNodesResponse(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response);
    void AddTopicError(TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode);
    void RespondIfRequired(const NActors::TActorContext& ctx);
    void AddProxyNodeToBrokers();
    void AddCurrentNodeToBrokers();
    void ProcessTopics();

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocationResponse, HandleResponse);
            HFunc(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse, HandleNodesResponse);
        }
    }

    TString LogPrefix() const;

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TMetadataRequestData> Message;
    const bool WithProxy;

    ui64 PendingResponses = 0;

    TMetadataResponseData::TPtr Response;
    THashMap<TActorId, TVector<ui64>> TopicIndexes;
    THashSet<ui64> AllClusterNodes;
    EKafkaErrors ErrorCode = EKafkaErrors::NONE_ERROR;
};

} // namespace NKafka
