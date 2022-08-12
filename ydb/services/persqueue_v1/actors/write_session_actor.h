#pragma once

#include "events.h"
#include "persqueue_utils.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/kqp/kqp.h>

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>


namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQWriteServiceActorID() {
    return TActorId(0, "PQWriteSvc");
}

class TWriteSessionActor : public NActors::TActorBootstrapped<TWriteSessionActor> {
    using IContext = NGRpcServer::IGRpcStreamingContext<PersQueue::V1::StreamingWriteClientMessage, PersQueue::V1::StreamingWriteServerMessage>;
    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

// Codec ID size in bytes
static constexpr ui32 CODEC_ID_SIZE = 1;

public:
    TWriteSessionActor(NKikimr::NGRpcService::TEvStreamPQWriteRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache,
                       TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
                       const NPersQueue::TTopicsListController& topicsController);
    ~TWriteSessionActor();

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_WRITE; }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup);

            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            CFunc(IContext::TEvNotifiedWhenDone::EventType, HandleDone);
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvPQProxy::TEvWriteInit,  Handle)
            HFunc(TEvPQProxy::TEvWrite, Handle)
            HFunc(TEvPQProxy::TEvUpdateToken, Handle)
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

        default:
            break;
        };
    }


    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void HandleDone(const TActorContext &ctx);

    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);


    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr &ev, const TActorContext &ctx);

    void CheckACL(const TActorContext& ctx);
    // Requests fresh ACL from 'SchemeCache'
    void InitCheckSchema(const TActorContext& ctx, bool needWaitSchema = false);
    void Handle(TEvPQProxy::TEvWriteInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvWrite::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvUpdateToken::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx);
    void LogSession(const TActorContext& ctx);
    void InitAfterDiscovery(const TActorContext& ctx);
    void DiscoverPartition(const NActors::TActorContext& ctx);
    void SendSelectPartitionRequest(ui32 hash, const TString& topic, const NActors::TActorContext& ctx);

    void UpdatePartition(const NActors::TActorContext& ctx);
    void RequestNextPartition(const NActors::TActorContext& ctx);

    void ProceedPartition(const ui32 partition, const NActors::TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeUpdateSourceIdMetadataRequest(ui32 hash, const TString& topic,
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
    void HandleWakeup(const NActors::TActorContext& ctx);

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);

    void CheckFinish(const NActors::TActorContext& ctx);

    void GenerateNextWriteRequest(const NActors::TActorContext& ctx);

    void SetupCounters();
    void SetupCounters(const TString& cloudId, const TString& dbId, const TString& folderId);

private:
    std::unique_ptr<NKikimr::NGRpcService::TEvStreamPQWriteRequest> Request;

    enum EState {
        ES_CREATED = 1,
        ES_WAIT_SCHEME_1 = 2,
        ES_WAIT_SCHEME_2 = 3,
        ES_WAIT_TABLE_REQUEST_1 = 4,
        ES_WAIT_NEXT_PARTITION = 5,
        ES_WAIT_TABLE_REQUEST_2 = 6,
        ES_WAIT_TABLE_REQUEST_3 = 7,
        ES_WAIT_WRITER_INIT = 8,
        ES_INITED = 9
        //TODO: filter
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
    ui32 CompatibleHash;

    TString OwnerCookie;
    TString UserAgent;

    ui32 NumReserveBytesRequests;

    THolder<TAclWrapper> ACL;

    struct TWriteRequestBatchInfo: public TSimpleRefCount<TWriteRequestBatchInfo> {
        using TPtr = TIntrusivePtr<TWriteRequestBatchInfo>;

        // Source requests from user (grpc session object)
        std::deque<THolder<TEvPQProxy::TEvWrite>> UserWriteRequests;

        // Formed write request's size
        ui64 ByteSize = 0;

        // Formed write request's cookie
        ui64 Cookie = 0;
    };

    // Nonprocessed source client requests
    std::deque<THolder<TEvPQProxy::TEvWrite>> Writes;

    // Formed, but not sent, batch requests to partition actor
    std::deque<TWriteRequestBatchInfo::TPtr> FormedWrites;

    // Requests that is already sent to partition actor
    std::deque<TWriteRequestBatchInfo::TPtr> SentMessages;


    bool WritesDone;

    THashMap<ui32, ui64> PartitionToTablet;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    NKikimr::NPQ::TMultiCounter BytesInflight;
    NKikimr::NPQ::TMultiCounter BytesInflightTotal;

    ui64 BytesInflight_;
    ui64 BytesInflightTotal_;

    bool NextRequestInited;

    NKikimr::NPQ::TMultiCounter SessionsCreated;
    NKikimr::NPQ::TMultiCounter SessionsActive;

    NKikimr::NPQ::TMultiCounter Errors;

    ui64 NextRequestCookie;

    TIntrusivePtr<NACLib::TUserToken> Token;
    TString Auth;
    // Got 'update_token_request', authentication or authorization in progress or 'update_token_response' is not sent yet. Only single 'update_token_request' is allowed inflight
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

    PersQueue::V1::StreamingWriteClientMessage::InitRequest InitRequest;
};

}
