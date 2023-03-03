#pragma once

#include "events.h"
#include "persqueue_utils.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/services/lib/actors/pq_rl_helpers.h>
#include <ydb/services/metadata/service.h>


namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQWriteServiceActorID() {
    return TActorId(0, "PQWriteSvc");
}

template<bool UseMigrationProtocol>
class TWriteSessionActor
    : public NActors::TActorBootstrapped<TWriteSessionActor<UseMigrationProtocol>>
    , private TRlHelpers
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

    struct TWriteRequestInfo: public TSimpleRefCount<TWriteRequestInfo> {
        using TPtr = TIntrusivePtr<TWriteRequestInfo>;

        explicit TWriteRequestInfo(ui64 cookie)
            : PartitionWriteRequest(new NPQ::TEvPartitionWriter::TEvWriteRequest(cookie))
            , Cookie(cookie)
            , ByteSize(0)
            , RequiredQuota(0)
        {
        }

        // Source requests from user (grpc session object)
        std::deque<THolder<TEvWrite>> UserWriteRequests;

        // Partition write request
        THolder<NPQ::TEvPartitionWriter::TEvWriteRequest> PartitionWriteRequest;

        // Formed write request's cookie
        ui64 Cookie;

        // Formed write request's size
        ui64 ByteSize;

        // Quota in term of RUs
        ui64 RequiredQuota;
    };

// Codec ID size in bytes
static constexpr ui32 CODEC_ID_SIZE = 1;

public:
    TWriteSessionActor(TEvStreamWriteRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache,
                       TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
                       const NPersQueue::TTopicsListController& topicsController);
    ~TWriteSessionActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_WRITE; }
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
            HFunc(TEvPersQueue::TEvGetPartitionIdForWriteResponse, Handle)

            HFunc(TEvDescribeTopicsResponse, Handle)

            HFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);

        default:
            break;
        };
    }


    void Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void HandleDone(const TActorContext &ctx);

    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);


    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr &ev, const NActors::TActorContext& ctx);
    void TryCloseSession(const TActorContext& ctx);

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr &ev, const NActors::TActorContext& ctx);

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
    ui32 CalculateFirstClassPartition(const TActorContext& ctx);
    void SendCreateManagerRequest(const TActorContext& ctx);
    void DiscoverPartition(const NActors::TActorContext& ctx);
    TString GetDatabaseName(const NActors::TActorContext& ctx);
    void StartSession(const NActors::TActorContext& ctx);
    void SendSelectPartitionRequest(const TString& topic, const NActors::TActorContext& ctx);

    void UpdatePartition(const NActors::TActorContext& ctx);
    void RequestNextPartition(const NActors::TActorContext& ctx);

    void ProceedPartition(const ui32 partition, const NActors::TActorContext& ctx);

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateSourceIdMetadataRequest(const TString& topic,
                                                                             const NActors::TActorContext& ctx);
    void SendUpdateSrcIdsRequests(const TActorContext& ctx);
    //void InitCheckACL(const TActorContext& ctx);

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);

    void CheckFinish(const NActors::TActorContext& ctx);

    void PrepareRequest(THolder<TEvWrite>&& ev, const TActorContext& ctx);
    void SendRequest(typename TWriteRequestInfo::TPtr&& request, const TActorContext& ctx);

    void SetupCounters();
    void SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId);

private:
    std::unique_ptr<TEvStreamWriteRequest> Request;

    enum EState {
        ES_CREATED = 1,
        ES_WAIT_SCHEME = 2,
        ES_WAIT_SESSION = 3,
        ES_WAIT_TABLE_REQUEST_1 = 4,
        ES_WAIT_NEXT_PARTITION = 5,
        ES_WAIT_TABLE_REQUEST_2 = 6,
        ES_WAIT_WRITER_INIT = 7,
        ES_INITED = 8,
        ES_DYING = 9
    };

    EState State;
    TActorId SchemeCache;
    TActorId Writer;

    TString PeerName;
    ui64 Cookie;

    NPersQueue::TTopicsListController TopicsController;
    NPersQueue::TDiscoveryConverterPtr DiscoveryConverter;
    NPersQueue::TTopicConverterPtr FullConverter;
    ui32 Partition;
    ui32 PreferedPartition;
    bool PartitionFound = false;
    // 'SourceId' is called 'MessageGroupId' since gRPC data plane API v1
    TString SourceId; // TODO: Replace with 'MessageGroupId' everywhere
    NPQ::NSourceIdEncoding::TEncodedSourceId EncodedSourceId;

    TString OwnerCookie;
    TString UserAgent;

    THolder<TAclWrapper> ACL;

    // Future batch request to partition actor
    typename TWriteRequestInfo::TPtr PendingRequest;
    // Request that is waiting for quota
    typename TWriteRequestInfo::TPtr PendingQuotaRequest;
    // Quoted, but not sent requests
    std::deque<typename TWriteRequestInfo::TPtr> QuotedRequests;
    // Requests that is sent to partition actor, but not accepted
    std::deque<typename TWriteRequestInfo::TPtr> SentRequests;
    // Accepted requests
    std::deque<typename TWriteRequestInfo::TPtr> AcceptedRequests;


    bool WritesDone;

    THashMap<ui32, ui64> PartitionToTablet;

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

    ui64 BalancerTabletId;
    TActorId PipeToBalancer;

    // PQ tablet configuration that we get at the time of session initialization
    NKikimrPQ::TPQTabletConfig InitialPQTabletConfig;

    NKikimrPQClient::TDataChunk InitMeta;
    TString LocalDC;
    TString ClientDC;
    TString SelectSourceIdQuery;
    TString UpdateSourceIdQuery;
    TString TxId;
    TString KqpSessionId;
    ui32 SelectSrcIdsInflight = 0;
    ui64 MaxSrcIdAccessTime = 0;

    TInstant LastSourceIdUpdate;
    ui64 SourceIdCreateTime;
    ui32 SourceIdUpdatesInflight = 0;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;

    TInitRequest InitRequest;
    NPQ::ESourceIdTableGeneration SrcIdTableGeneration;

};

}

/////////////////////////////////////////
// Implementation
#define WRITE_SESSION_ACTOR_IMPL
#include "write_session_actor.ipp"
#undef WRITE_SESSION_ACTOR_IMPL
