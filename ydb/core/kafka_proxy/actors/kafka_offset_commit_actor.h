#include "actors.h"

#include "ydb/core/base/tablet_pipe.h"
#include "ydb/core/grpc_services/local_rpc/local_rpc.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include "ydb/public/lib/base/msgbus_status.h"
#include <ydb/services/persqueue_v1/actors/events.h>
#include "ydb/services/persqueue_v1/actors/persqueue_utils.h"
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>


namespace NKafka {
using namespace NKikimr;

class TKafkaOffsetCommitActor: public NActors::TActorBootstrapped<TKafkaOffsetCommitActor> {

struct TRequestInfo {
    TString TopicName = "";
    ui64 PartitionId = 0;
    bool Done = false;

    TRequestInfo(const TString& topicName, ui64 partitionId)
        : TopicName(topicName), PartitionId(partitionId) {}
};

public:
    using TBase = NActors::TActorBootstrapped<TKafkaOffsetCommitActor>;
    TKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , Response(new TOffsetCommitResponseData()) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    TString LogPrefix();
    void Die(const TActorContext& ctx) override;

    STATEFN(StateWork) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

    void Handle(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void AddPartitionResponse(EKafkaErrors error, const TString& topicName, ui64 partitionId, const TActorContext& ctx);
    void ProcessPipeProblem(ui64 tabletId, const TActorContext& ctx);
    void SendFailedForAllPartitions(EKafkaErrors error, const TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TOffsetCommitRequestData> Message;
    const TOffsetCommitResponseData::TPtr Response;

    ui64 PendingResponses = 0;
    ui64 NextCookie = 0;
    std::unordered_map<ui64, TVector<ui64>> TabletIdToCookies;
    std::unordered_map<ui64, TRequestInfo> CookieToRequestInfo;
    std::unordered_map<TString, ui64> ResponseTopicIds;
    NKikimr::NGRpcProxy::TTopicInitInfoMap TopicAndTablets;
    std::unordered_map<ui64, TActorId> TabletIdToPipe;
    TActorId AuthInitActor;
    EKafkaErrors Error = NONE_ERROR;

    static constexpr NTabletPipe::TClientRetryPolicy RetryPolicyForPipes = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
};

} // NKafka
