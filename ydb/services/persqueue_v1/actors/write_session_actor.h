#pragma once

#include "events.h"
#include "partition_writer.h"
#include "persqueue_utils.h"
#include "write_request_info.h"
#include "partition_writer_cache_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/services/metadata/service.h>


namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQWriteServiceActorID() {
    return TActorId(0, "PQWriteSvc");
}

template<bool UseMigrationProtocol>
class TWriteSessionActor
    : public NActors::TActorBootstrapped<TWriteSessionActor<UseMigrationProtocol>>
    , private NPQ::TRlHelpers
{
    using TSelf = TWriteSessionActor<UseMigrationProtocol>;
    using TClientMessage = std::conditional_t<UseMigrationProtocol, PersQueue::V1::StreamingWriteClientMessage,
                                              Topic::StreamWriteMessage::FromClient>;
    using TServerMessage = std::conditional_t<UseMigrationProtocol, PersQueue::V1::StreamingWriteServerMessage,
                                              Topic::StreamWriteMessage::FromServer>;

    using TInitRequest =
        std::conditional_t<UseMigrationProtocol, PersQueue::V1::StreamingWriteClientMessage::InitRequest,
                           Topic::StreamWriteMessage::InitRequest>;

    using TEvWriteInit =
        std::conditional_t<UseMigrationProtocol, TEvPQProxy::TEvWriteInit, TEvPQProxy::TEvTopicWriteInit>;
    using TEvWrite = std::conditional_t<UseMigrationProtocol, TEvPQProxy::TEvWrite, TEvPQProxy::TEvTopicWrite>;
    using TEvUpdateToken =
        std::conditional_t<UseMigrationProtocol, TEvPQProxy::TEvUpdateToken, TEvPQProxy::TEvTopicUpdateToken>;
    using TEvStreamWriteRequest =
        std::conditional_t<UseMigrationProtocol, NKikimr::NGRpcService::TEvStreamPQWriteRequest,
                           NKikimr::NGRpcService::TEvStreamTopicWriteRequest>;

    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

    using TWriteRequestInfo = TWriteRequestInfoImpl<TEvWrite>;

    // Codec ID size in bytes
    static constexpr ui32 CODEC_ID_SIZE = 1;

    TString UserAgent = UseMigrationProtocol ? "pqv1 server" : "topic server";
    TString SdkBuildInfo;
    static constexpr auto ProtoName = UseMigrationProtocol ? "v1" : "topic";

public:
    TWriteSessionActor(TEvStreamWriteRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache,
                       TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
                       const NPersQueue::TTopicsListController& topicsController);
    ~TWriteSessionActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_PQ_WRITE;
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, Handle);

            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            CFunc(IContext::TEvNotifiedWhenDone::EventType, HandleDone);
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvWriteInit,  Handle)
            HFunc(TEvWrite, Handle)
            HFunc(TEvUpdateToken, Handle)
            HFunc(TEvPQProxy::TEvDone, Handle)

            HFunc(TEvDescribeTopicsResponse, Handle)

            HFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);

            HFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

            HFunc(NPQ::TEvPartitionChooser::TEvChooseResult, Handle);
            HFunc(NPQ::TEvPartitionChooser::TEvChooseError, Handle);

        default:
            break;
        };
    }


    void Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void HandleDone(const TActorContext &ctx);

    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);

    void CheckACL(const TActorContext& ctx);
    void RecheckACL(const TActorContext& ctx);
    // Requests fresh ACL from 'SchemeCache'
    void InitCheckSchema(const TActorContext& ctx, bool needWaitSchema = false);
    void Handle(typename TEvWriteInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(typename TEvWrite::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(typename TEvUpdateToken::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx);
    void LogSession(const TActorContext& ctx);
    void InitAfterDiscovery(const TActorContext& ctx);

    void ProceedPartition(const ui32 partition, const NActors::TActorContext& ctx);

    //void InitCheckACL(const TActorContext& ctx);

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx);
    void MakeAndSentInitResponse(const TMaybe<ui64>& maxSeqNo, const TActorContext& ctx);

    void Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx);
    void ProcessWriteResponse(const NKikimrClient::TPersQueuePartitionResponse& response, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

    void DiscoverPartition(const NActors::TActorContext& ctx);
    void Handle(NPQ::TEvPartitionChooser::TEvChooseResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NPQ::TEvPartitionChooser::TEvChooseError::TPtr& ev, const NActors::TActorContext& ctx);

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);

    void CheckFinish(const NActors::TActorContext& ctx);

    void PrepareRequest(THolder<TEvWrite>&& ev, const TActorContext& ctx);
    void SendWriteRequest(typename TWriteRequestInfo::TPtr&& request, const TActorContext& ctx);

    void SetupBytesWrittenByUserAgentCounter(const TString& topicPath);
    void SetupCounters();
    void SetupCounters(const TActorContext& ctx, const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId);

private:
    void CreatePartitionWriterCache(const TActorContext& ctx);
    void DestroyPartitionWriterCache(const TActorContext& ctx);

    std::unique_ptr<TEvStreamWriteRequest> Request;

    enum EState {
        ES_CREATED = 1,
        ES_WAIT_SCHEME = 2,
        ES_WAIT_PARTITION = 3,
        ES_WAIT_WRITER_INIT = 7,
        ES_INITED = 8,
        ES_DYING = 9
    };

    EState State;
    TActorId SchemeCache;

    TString PeerName;
    ui64 Cookie;

    NPersQueue::TTopicsListController TopicsController;
    NPersQueue::TDiscoveryConverterPtr DiscoveryConverter;
    NPersQueue::TTopicConverterPtr FullConverter;
    ui32 Partition;
    ui64 PartitionTabletId;
    ui32 PreferedPartition;
    std::optional<ui32> ExpectedGeneration;
    std::optional<ui64> InitialSeqNo;

    bool PartitionFound = false;
    // 'SourceId' is called 'MessageGroupId' since gRPC data plane API v1
    TString SourceId; // TODO: Replace with 'MessageGroupId' everywhere
    bool UseDeduplication = true;

    TString OwnerCookie;

    THolder<TAclWrapper> ACL;

    // Future batch request to partition actor
    std::deque<typename TWriteRequestInfo::TPtr> PendingRequests;
    // Request that is waiting for quota
    typename TWriteRequestInfo::TPtr PendingQuotaRequest;

    // Requests that is sent to partition actor, but not accepted
    std::deque<typename TWriteRequestInfo::TPtr> SentRequests;
    // Accepted requests
    std::deque<typename TWriteRequestInfo::TPtr> AcceptedRequests;

    bool WritesDone;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    NKikimr::NPQ::TMultiCounter BytesInflight;
    NKikimr::NPQ::TMultiCounter BytesInflightTotal;

    ui64 BytesInflight_;
    ui64 BytesInflightTotal_;

    bool NextRequestInited;
    ui64 NextRequestCookie;

    ui64 PayloadBytes;

    NKikimr::NPQ::TMultiCounter SessionsCreated;
    NKikimr::NPQ::TMultiCounter SessionsActive;

    NKikimr::NPQ::TMultiCounter Errors;
    std::vector<NKikimr::NPQ::TMultiCounter> CodecCounters;

    NYdb::NPersQueue::TCounterPtr BytesWrittenByUserAgent;

    TIntrusiveConstPtr<NACLib::TUserToken> Token;
    TString Auth;
    // Got 'update_token_request', authentication or authorization in progress,
    // or 'update_token_response' is not sent yet.
    // Only single 'update_token_request' is allowed inflight.
    bool UpdateTokenInProgress;
    bool UpdateTokenAuthenticated;
    bool ACLCheckInProgress;
    bool FirstACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;
    TInstant LogSessionDeadline;

    NKikimrSchemeOp::TPersQueueGroupDescription Config;
    // PQ tablet configuration that we get at the time of session initialization
    NKikimrPQ::TPQTabletConfig InitialPQTabletConfig;

    NKikimrPQClient::TDataChunk InitMeta;
    TString LocalDC;
    TString ClientDC;

    TInstant LastSourceIdUpdate;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;

    TInitRequest InitRequest;

    TActorId PartitionWriterCache;
    TActorId PartitionChooser;

    bool SessionClosed = false;
};

}
