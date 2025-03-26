#include "actors.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/core/discovery/discovery.h>
#include <ydb/core/kafka_proxy/kafka_listener.h>
#include <ydb/core/persqueue/events/internal.h>


namespace NKafka {

class TKafkaMetadataActor: public NActors::TActorBootstrapped<TKafkaMetadataActor> {
public:
    TKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TMetadataRequestData>& message,
                        const TActorId& discoveryCacheActor)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , WithProxy(context->Config.HasProxy() && !context->Config.GetProxy().GetHostname().Empty())
        , Response(new TMetadataResponseData())
        , DiscoveryCacheActor(discoveryCacheActor)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;

    struct TNodeInfo {
        TString Host;
        ui32 Port;
    };

    TActorId SendTopicRequest(const TString& topic);
    void HandleLocationResponse(TEvLocationResponse::TPtr ev, const NActors::TActorContext& ctx);
    void HandleNodesResponse(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev,
                             const NActors::TActorContext& ctx);
    void HandleDiscoveryData(NKikimr::TEvDiscovery::TEvDiscoveryData::TPtr& ev);
    void HandleDiscoveryError(NKikimr::TEvDiscovery::TEvError::TPtr& ev);
    void HandleListTopics(NKikimr::TEvPQ::TEvListAllTopicsResponse::TPtr& ev);

    void AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response,
                          const TVector<TNodeInfo*>& nodes);
    void AddTopicError(TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode);
    void RespondIfRequired(const NActors::TActorContext& ctx);
    void AddProxyNodeToBrokers();
    void AddBroker(ui64 nodeId, const TString& host, ui64 port);
    void RequestICNodeCache();
    void ProcessTopicsFromRequest();
    void SendDiscoveryRequest();
    void ProcessDiscoveryData(NKikimr::TEvDiscovery::TEvDiscoveryData::TPtr& ev);
    TVector<TNodeInfo*> CheckTopicNodes(TEvLocationResponse* response);

    void AddTopic(const TString& topic, ui64 index);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocationResponse, HandleLocationResponse);
            HFunc(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse, HandleNodesResponse);
            hFunc(NKikimr::TEvDiscovery::TEvDiscoveryData, HandleDiscoveryData);
            hFunc(NKikimr::TEvDiscovery::TEvError, HandleDiscoveryError);
            hFunc(NKikimr::TEvPQ::TEvListAllTopicsResponse, HandleListTopics);
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

    TActorId DiscoveryCacheActor;
    bool NeedAllNodes = false;
    bool HaveError = false;
    bool FallbackToIcDiscovery = false;
    TMap<ui64, TSimpleSharedPtr<TEvLocationResponse>> PendingTopicResponses;

    THashMap<ui64, TNodeInfo> Nodes;
    THashMap<TString, TActorId> PartitionActors;
    THashSet<ui64> HaveBrokers;

};

} // namespace NKafka
