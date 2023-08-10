#include "../kafka_events.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>

namespace NKafka {

class TKafkaMetadataActor: public NActors::TActorBootstrapped<TKafkaMetadataActor> {
public:
    TKafkaMetadataActor(const TActorId& parent, const NACLib::TUserToken* userToken, const ui64 correlationId, const TMetadataRequestData* message, const NKikimrConfig::TKafkaProxyConfig& config)
        : Parent(parent)
        , UserToken(userToken)
        , CorrelationId(correlationId)
        , Message(message)
        , Config(config)
        , Response(new TMetadataResponseData())
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    using TEvLocationResponse = NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvPartitionLocationResponse;

    TActorId SendTopicRequest(const TMetadataRequestData::TMetadataRequestTopic& topicRequest);
    void HandleResponse(TEvLocationResponse::TPtr ev, const TActorContext& ctx);

    void AddTopicResponse(TMetadataResponseData::TMetadataResponseTopic& topic, TEvLocationResponse* response);
    void AddTopicError(TMetadataResponseData::TMetadataResponseTopic& topic, EKafkaErrors errorCode);
    void RespondIfRequired(const TActorContext& ctx);
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocationResponse, HandleResponse);
        }
    }

    TString LogPrefix() const;

private:
    const TActorId Parent;
    const NACLib::TUserToken* UserToken;
    const ui64 CorrelationId;
    const TMetadataRequestData* Message;
    const NKikimrConfig::TKafkaProxyConfig& Config;

    ui64 PendingResponses = 0;

    TMetadataResponseData::TPtr Response;
    THashMap<TActorId, TVector<ui64>> TopicIndexes;
    THashSet<ui64> AllClusterNodes;
};

} // namespace NKafka
