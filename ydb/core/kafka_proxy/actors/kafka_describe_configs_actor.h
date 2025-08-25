#include "actors.h"
#include "control_plane_common.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

enum EConfigSource {
    UNKNOWN = 0,
    DYNAMIC_TOPIC_CONFIG = 1,
    DYNAMIC_BROKER_CONFIG = 2,
    DYNAMIC_DEFAULT_BROKER_CONFIG = 3,
    STATIC_BROKER_CONFIG = 4,
    DEFAULT_CONFIG = 5,
    DYNAMIC_BROKER_LOGGER_CONFIG = 6,
};

class TDescribeConfigsRequest: public TKafkaTopicRequestCtx {
public:
    using TKafkaTopicRequestCtx::TKafkaTopicRequestCtx;

    static TDescribeConfigsRequest* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
        return static_cast<TDescribeConfigsRequest*>(&(*request));
    }

};

class TKafkaDescribeConfigsActor: public NActors::TActorBootstrapped<TKafkaDescribeConfigsActor> {
public:
    TKafkaDescribeConfigsActor(
            const TContext::TPtr context,
            const ui64 correlationId,
            const TMessagePtr<TDescribeConfigsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);
    void Handle(const TEvKafka::TEvTopicDescribeResponse::TPtr& ev);
    void Reply();

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKafka::TEvTopicDescribeResponse, Handle);
        }
    }

private:
    void AddDescribeResponse(TDescribeConfigsResponseData::TPtr& response, TEvKafka::TEvTopicDescribeResponse* ev,
                             const TString& topicName, EKafkaErrors status, const TString& message);
    TStringBuilder InputLogMessage();

    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TDescribeConfigsRequestData> Message;
    ui32 InflyTopics = 0;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvTopicDescribeResponse>> TopicNamesToResponses;
};



class TKafkaDescribeTopicActor : public NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TKafkaDescribeTopicActor, TDescribeConfigsRequest>
                               , public NKikimr::NGRpcProxy::V1::TCdcStreamCompatible {
    using TBase = NKikimr::NGRpcProxy::V1::TPQGrpcSchemaBase<TKafkaDescribeTopicActor, TDescribeConfigsRequest>;

public:
    TKafkaDescribeTopicActor(
            TActorId requester,
            TIntrusiveConstPtr<NACLib::TUserToken> userToken,
            TString topicPath,
            TString databaseName);

    ~TKafkaDescribeTopicActor() = default;

    void SendResult(const EKafkaErrors status, const TString& message, const google::protobuf::Message& result);
    void StateWork(TAutoPtr<IEventHandle>& ev);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

protected:
    const TString TopicPath;

private:
    const TActorId Requester;
    const std::shared_ptr<TString> SerializedToken;
    TIntrusiveConstPtr<NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
};


} // NKafka
