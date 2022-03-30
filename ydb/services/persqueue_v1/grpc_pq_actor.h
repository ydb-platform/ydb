#pragma once

#include "persqueue_utils.h"

#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_calls.h>


#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/base/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>


#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/core/kqp/kqp.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/services/lib/actors/type_definitions.h>

#include <util/generic/guid.h>
#include <util/system/compiler.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace Ydb;

using namespace NKikimr::NGRpcService;

PersQueue::ErrorCode::ErrorCode ConvertOldCode(const NPersQueue::NErrorCode::EErrorCode code);

void FillIssue(Ydb::Issue::IssueMessage* issue, const PersQueue::ErrorCode::ErrorCode errorCode, const TString& errorReason);

const TString& TopicPrefix(const TActorContext& ctx);

static const TDuration CHECK_ACL_DELAY = TDuration::Minutes(5);

// Codec ID size in bytes
constexpr ui32 CODEC_ID_SIZE = 1;


template<typename TItem0, typename... TItems>
bool AllEqual(const TItem0& item0, const TItems&... items) {
    return ((items == item0) && ... && true);
}

static inline bool InternalErrorCode(PersQueue::ErrorCode::ErrorCode errorCode) {
    switch(errorCode) {
        case PersQueue::ErrorCode::UNKNOWN_TOPIC:
        case PersQueue::ErrorCode::ERROR:
        case PersQueue::ErrorCode::INITIALIZING:
        case PersQueue::ErrorCode::OVERLOAD:
        case PersQueue::ErrorCode::WRITE_ERROR_DISK_IS_FULL:
            return true;
        default:
            return false;
    }
    return false;
}

struct TPartitionId {
    NPersQueue::TConverterPtr TopicConverter;
    ui64 Partition;
    ui64 AssignId;

    bool operator < (const TPartitionId& rhs) const {
        return std::make_tuple(AssignId, Partition, TopicConverter->GetClientsideName()) <
               std::make_tuple(rhs.AssignId, rhs.Partition, rhs.TopicConverter->GetClientsideName());
    }
};



IOutputStream& operator <<(IOutputStream& out, const TPartitionId& partId);

struct TCommitCookie {
    ui64 AssignId;
    ui64 Cookie;
};


struct TEvPQProxy {
    enum EEv {
        EvWriteInit = EventSpaceBegin(TKikimrEvents::ES_PQ_PROXY_NEW), // TODO: Replace 'NEW' with version or something
        EvWrite,
        EvDone,
        EvReadInit,
        EvRead,
        EvCloseSession,
        EvPartitionReady,
        EvReadResponse,
        EvCommitCookie,
        EvCommitDone,
        EvStartRead,
        EvReleasePartition,
        EvReleased,
        EvPartitionReleased,
        EvLockPartition,
        EvRestartPipe,
        EvDieCommand,
        EvPartitionStatus,
        EvAuth,
        EvReadSessionStatus,
        EvReadSessionStatusResponse,
        EvAuthResultOk,
        EvUpdateClusters,
        EvQueryCompiled,
        EvSessionDead,
        EvSessionSetPreferredCluster,
        EvScheduleUpdateClusters,
        EvDeadlineExceeded,
        EvGetStatus,
        EvUpdateToken,
        EvCommitRange,
        EvEnd
    };


    struct TEvReadSessionStatus : public TEventPB<TEvReadSessionStatus, NKikimrPQ::TReadSessionStatus, EvReadSessionStatus> {
    };

    struct TEvReadSessionStatusResponse : public TEventPB<TEvReadSessionStatusResponse, NKikimrPQ::TReadSessionStatusResponse, EvReadSessionStatusResponse> {
    };


    struct TEvAuthResultOk : public NActors::TEventLocal<TEvAuthResultOk, EvAuthResultOk> {
        TEvAuthResultOk(const TTopicTabletsPairs&& topicAndTablets)
            : TopicAndTablets(std::move(topicAndTablets))
        { }

        TTopicTabletsPairs TopicAndTablets;
    };

    struct TEvSessionSetPreferredCluster : public NActors::TEventLocal<TEvSessionSetPreferredCluster, EvSessionSetPreferredCluster> {
        TEvSessionSetPreferredCluster(const ui64 cookie, const TString& preferredCluster)
            : Cookie(cookie)
            , PreferredCluster(preferredCluster)
        {}
        const ui64 Cookie;
        const TString PreferredCluster;
    };

    struct TEvSessionDead : public NActors::TEventLocal<TEvSessionDead, EvSessionDead> {
        TEvSessionDead(const ui64 cookie)
            : Cookie(cookie)
        { }

        const ui64 Cookie;
    };

    struct TEvScheduleUpdateClusters : public NActors::TEventLocal<TEvScheduleUpdateClusters, EvScheduleUpdateClusters> {
        TEvScheduleUpdateClusters()
        { }
    };


    struct TEvUpdateClusters : public NActors::TEventLocal<TEvUpdateClusters, EvUpdateClusters> {
        TEvUpdateClusters(const TString& localCluster, bool enabled, const TVector<TString>& clusters)
            : LocalCluster(localCluster)
            , Enabled(enabled)
            , Clusters(clusters)
        { }

        const TString LocalCluster;
        const bool Enabled;
        const TVector<TString> Clusters;
    };

    struct TEvQueryCompiled : public NActors::TEventLocal<TEvQueryCompiled, EvQueryCompiled> {
        TEvQueryCompiled(const TString& selectQ, const TString& updateQ, const TString& deleteQ)
            : SelectQ(selectQ)
            , UpdateQ(updateQ)
            , DeleteQ(deleteQ)
        { }

        const TString SelectQ, UpdateQ, DeleteQ;
    };



    struct TEvWriteInit : public NActors::TEventLocal<TEvWriteInit, EvWriteInit> {
        TEvWriteInit(PersQueue::V1::StreamingWriteClientMessage&& req, const TString& peerName)
            : Request(std::move(req))
            , PeerName(peerName)
        { }

        PersQueue::V1::StreamingWriteClientMessage Request;
        TString PeerName;
    };

    struct TEvWrite : public NActors::TEventLocal<TEvWrite, EvWrite> {
        explicit TEvWrite(PersQueue::V1::StreamingWriteClientMessage&& req)
            : Request(std::move(req))
        { }

        PersQueue::V1::StreamingWriteClientMessage Request;
    };

    struct TEvDone : public NActors::TEventLocal<TEvDone, EvDone> {
        TEvDone()
        { }
    };

    struct TEvReadInit : public NActors::TEventLocal<TEvReadInit, EvReadInit> {
        TEvReadInit(const PersQueue::V1::MigrationStreamingReadClientMessage& req, const TString& peerName)
            : Request(req)
            , PeerName(peerName)
        { }

        PersQueue::V1::MigrationStreamingReadClientMessage Request;
        TString PeerName;
    };

    struct TEvRead : public NActors::TEventLocal<TEvRead, EvRead> {
        explicit TEvRead(const TString& guid = CreateGuidAsString(), ui64 maxCount = 0, ui64 maxSize = 0, ui64 maxTimeLagMs = 0, ui64 readTimestampMs = 0)
            : Guid(guid)
            , MaxCount(maxCount)
            , MaxSize(maxSize)
            , MaxTimeLagMs(maxTimeLagMs)
            , ReadTimestampMs(readTimestampMs)
        { }

        const TString Guid;
        ui64 MaxCount;
        ui64 MaxSize;
        ui64 MaxTimeLagMs;
        ui64 ReadTimestampMs;
    };

    struct TEvUpdateToken : public NActors::TEventLocal<TEvUpdateToken, EvUpdateToken> {
        explicit TEvUpdateToken(PersQueue::V1::StreamingWriteClientMessage&& req)
            : Request(std::move(req))
        { }

        PersQueue::V1::StreamingWriteClientMessage Request;
    };

    struct TEvCloseSession : public NActors::TEventLocal<TEvCloseSession, EvCloseSession> {
        TEvCloseSession(const TString& reason, const PersQueue::ErrorCode::ErrorCode errorCode)
            : Reason(reason)
            , ErrorCode(errorCode)
        { }

        const TString Reason;
        PersQueue::ErrorCode::ErrorCode ErrorCode;
    };

    struct TEvPartitionReady : public NActors::TEventLocal<TEvPartitionReady, EvPartitionReady> {
        TEvPartitionReady(const TPartitionId& partition, const ui64 wTime, const ui64 sizeLag,
                          const ui64 readOffset, const ui64 endOffset)
            : Partition(partition)
            , WTime(wTime)
            , SizeLag(sizeLag)
            , ReadOffset(readOffset)
            , EndOffset(endOffset)
        { }

        const TPartitionId Partition;
        ui64 WTime;
        ui64 SizeLag;
        ui64 ReadOffset;
        ui64 EndOffset;
    };

    struct TEvReadResponse : public NActors::TEventLocal<TEvReadResponse, EvReadResponse> {
        explicit TEvReadResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& resp, ui64 nextReadOffset, bool fromDisk, TDuration waitQuotaTime)
            : Response(std::move(resp))
            , NextReadOffset(nextReadOffset)
            , FromDisk(fromDisk)
            , WaitQuotaTime(waitQuotaTime)
        { }

        PersQueue::V1::MigrationStreamingReadServerMessage Response;
        ui64 NextReadOffset;
        bool FromDisk;
        TDuration WaitQuotaTime;
    };

    struct TCommitCookie {
        TVector<ui64> Cookies;
    };

    struct TCommitRange {
        TVector<std::pair<ui64, ui64>> Ranges;
    };

    struct TEvCommitCookie : public NActors::TEventLocal<TEvCommitCookie, EvCommitCookie> {
        explicit TEvCommitCookie(const ui64 assignId, const TCommitCookie&& commitInfo)
            : AssignId(assignId)
            , CommitInfo(std::move(commitInfo))
        { }

        ui64 AssignId;
        TCommitCookie CommitInfo;
    };

    struct TEvCommitRange : public NActors::TEventLocal<TEvCommitRange, EvCommitRange> {
        explicit TEvCommitRange(const ui64 assignId, const TCommitRange&& commitInfo)
            : AssignId(assignId)
            , CommitInfo(std::move(commitInfo))
        { }

        ui64 AssignId;
        TCommitRange CommitInfo;
    };




    struct TEvAuth : public NActors::TEventLocal<TEvAuth, EvAuth> {
        TEvAuth(const TString& auth)
            : Auth(auth)
        { }

        TString Auth;
    };

    struct TEvStartRead : public NActors::TEventLocal<TEvStartRead, EvStartRead> {
        TEvStartRead(const TPartitionId& partition, ui64 readOffset, ui64 commitOffset, bool verifyReadOffset)
            : Partition(partition)
            , ReadOffset(readOffset)
            , CommitOffset(commitOffset)
            , VerifyReadOffset(verifyReadOffset)
            , Generation(0)
        { }

        const TPartitionId Partition;
        ui64 ReadOffset;
        ui64 CommitOffset;
        bool VerifyReadOffset;
        ui64 Generation;
    };

    struct TEvReleased : public NActors::TEventLocal<TEvReleased, EvReleased> {
        TEvReleased(const TPartitionId& partition)
            : Partition(partition)
        { }

        const TPartitionId Partition;
    };

    struct TEvGetStatus : public NActors::TEventLocal<TEvGetStatus, EvGetStatus> {
        TEvGetStatus(const TPartitionId& partition)
            : Partition(partition)
        { }

        const TPartitionId Partition;
    };


    struct TEvCommitDone : public NActors::TEventLocal<TEvCommitDone, EvCommitDone> {
        explicit TEvCommitDone(const ui64 assignId, const ui64 startCookie, const ui64 lastCookie, const ui64 offset)
            : AssignId(assignId)
            , StartCookie(startCookie)
            , LastCookie(lastCookie)
            , Offset(offset)
        { }

        ui64 AssignId;
        ui64 StartCookie;
        ui64 LastCookie;
        ui64 Offset;
    };

    struct TEvReleasePartition : public NActors::TEventLocal<TEvReleasePartition, EvReleasePartition> {
        TEvReleasePartition()
        { }
    };

    struct TEvLockPartition : public NActors::TEventLocal<TEvLockPartition, EvLockPartition> {
        explicit TEvLockPartition(const ui64 readOffset, const ui64 commitOffset, bool verifyReadOffset, bool startReading)
            : ReadOffset(readOffset)
            , CommitOffset(commitOffset)
            , VerifyReadOffset(verifyReadOffset)
            , StartReading(startReading)
        { }

        ui64 ReadOffset;
        ui64 CommitOffset;
        bool VerifyReadOffset;
        bool StartReading;
    };


    struct TEvPartitionReleased : public NActors::TEventLocal<TEvPartitionReleased, EvPartitionReleased> {
        TEvPartitionReleased(const TPartitionId& partition)
            : Partition(partition)
        { }
        TPartitionId Partition;
    };


    struct TEvRestartPipe : public NActors::TEventLocal<TEvRestartPipe, EvRestartPipe> {
        TEvRestartPipe()
        { }
    };

    struct TEvDeadlineExceeded : public NActors::TEventLocal<TEvDeadlineExceeded, EvDeadlineExceeded> {
        TEvDeadlineExceeded(ui64 cookie)
            : Cookie(cookie)
        { }

        ui64 Cookie;
    };


    struct TEvDieCommand : public NActors::TEventLocal<TEvDieCommand, EvDieCommand> {
        TEvDieCommand(const TString& reason, const PersQueue::ErrorCode::ErrorCode errorCode)
        : Reason(reason)
        , ErrorCode(errorCode)
        { }

        TString Reason;
        PersQueue::ErrorCode::ErrorCode ErrorCode;
    };

    struct TEvPartitionStatus : public NActors::TEventLocal<TEvPartitionStatus, EvPartitionStatus> {
        TEvPartitionStatus(const TPartitionId& partition, const ui64 offset, const ui64 endOffset, const ui64 writeTimestampEstimateMs, bool init = true)
        : Partition(partition)
        , Offset(offset)
        , EndOffset(endOffset)
        , WriteTimestampEstimateMs(writeTimestampEstimateMs)
        , Init(init)
        { }

        TPartitionId Partition;
        ui64 Offset;
        ui64 EndOffset;
        ui64 WriteTimestampEstimateMs;
        bool Init;
    };

};



/// WRITE ACTOR
class TWriteSessionActor : public NActors::TActorBootstrapped<TWriteSessionActor> {
using IContext = NGRpcServer::IGRpcStreamingContext<PersQueue::V1::StreamingWriteClientMessage, PersQueue::V1::StreamingWriteServerMessage>;
using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

public:
    TWriteSessionActor(NKikimr::NGRpcService::TEvStreamPQWriteRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache,
                       TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
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
    NPersQueue::TConverterPtr TopicConverter;
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

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

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
    ui32 SelectSrcIdsInflight = 0;
    ui64 MaxSrcIdAccessTime = 0;

    TInstant LastSourceIdUpdate;
    ui64 SourceIdCreateTime;
    ui32 SourceIdUpdatesInflight = 0;

    TVector<NPQ::TLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;
};


class TReadInitAndAuthActor : public NActors::TActorBootstrapped<TReadInitAndAuthActor> {
using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

public:
     TReadInitAndAuthActor(const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
                           const TString& session, const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                           TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NACLib::TUserToken> token,
                           const NPersQueue::TTopicsToConverter& topics, const TString& localCluster);

    ~TReadInitAndAuthActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }

private:

    STRICT_STFUNC(StateFunc,
          HFunc(TEvDescribeTopicsResponse, HandleTopicsDescribeResponse)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleClientSchemeCacheResponse)
          HFunc(NActors::TEvents::TEvPoisonPill, HandlePoison)
    );

    void HandlePoison(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext& ctx) {
        Die(ctx);
    }

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code, const TActorContext& ctx);

    void DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate = false);
    bool ProcessTopicSchemeCacheResponse(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry,
                                         THashMap<TString, TTopicHolder>::iterator topicsIter, const TActorContext& ctx);
    void HandleClientSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void SendCacheNavigateRequest(const TActorContext& ctx, const TString& path);

    void HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void FinishInitialization(const NActors::TActorContext& ctx);
    bool CheckTopicACL(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& topic, const TActorContext& ctx);
    void CheckClientACL(const TActorContext& ctx);

    bool CheckACLPermissionsForNavigate(const TIntrusivePtr<TSecurityObject>& secObject,
                                        const TString& path, NACLib::EAccessRights rights,
                                        const TString& errorTextWhenAccessDenied,
                                        const TActorContext& ctx);

private:
    const TActorId ParentId;
    const ui64 Cookie;
    const TString Session;

    const TActorId MetaCacheId;
    const TActorId NewSchemeCache;

    const TString ClientId;
    const TString ClientPath;

    TIntrusivePtr<NACLib::TUserToken> Token;

    THashMap<TString, TTopicHolder> Topics; // topic -> info

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    bool DoCheckACL;

    TString LocalCluster;
};


class TReadSessionActor : public TActorBootstrapped<TReadSessionActor> {
using IContext = NGRpcServer::IGRpcStreamingContext<PersQueue::V1::MigrationStreamingReadClientMessage, PersQueue::V1::MigrationStreamingReadServerMessage>;
public:
     TReadSessionActor(NKikimr::NGRpcService::TEvStreamPQReadRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                       TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
                       const NPersQueue::TTopicsListController& topicsHandler);
    ~TReadSessionActor();

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }


    struct TTopicCounters {
        NKikimr::NPQ::TMultiCounter PartitionsLocked;
        NKikimr::NPQ::TMultiCounter PartitionsReleased;
        NKikimr::NPQ::TMultiCounter PartitionsToBeReleased;
        NKikimr::NPQ::TMultiCounter PartitionsToBeLocked;
        NKikimr::NPQ::TMultiCounter PartitionsInfly;
        NKikimr::NPQ::TMultiCounter Errors;
        NKikimr::NPQ::TMultiCounter Commits;
        NKikimr::NPQ::TMultiCounter WaitsForData;

        NKikimr::NPQ::TPercentileCounter CommitLatency;
        NKikimr::NPQ::TMultiCounter SLIBigLatency;
        NKikimr::NPQ::TMultiCounter SLITotal;
    };

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)

            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            CFunc(IContext::TEvNotifiedWhenDone::EventType, HandleDone);
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvPQProxy::TEvReadInit,  Handle) //from gRPC
            HFunc(TEvPQProxy::TEvReadSessionStatus, Handle) // from read sessions info builder proxy
            HFunc(TEvPQProxy::TEvRead, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvDone, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvCloseSession, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReady, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReleased, Handle) //from partitionActor

            HFunc(TEvPQProxy::TEvReadResponse, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvCommitCookie, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvCommitRange, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvStartRead, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvReleased, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvGetStatus, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvAuth, Handle) //from gRPC

            HFunc(TEvPQProxy::TEvCommitDone, Handle) //from PartitionActor
            HFunc(TEvPQProxy::TEvPartitionStatus, Handle) //from partitionActor

            HFunc(TEvPersQueue::TEvLockPartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvReleasePartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvError, Handle) //from Balancer

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

        default:
            break;
        };
    }

    bool WriteResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& response, bool finish = false);

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void HandleDone(const TActorContext &ctx);

    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);


    void Handle(TEvPQProxy::TEvReadInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReleased::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuth::TPtr& ev, const NActors::TActorContext& ctx);
    void ProcessAuth(const TString& auth, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvError::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    [[nodiscard]] bool ProcessBalancerDead(const ui64 tabletId, const NActors::TActorContext& ctx); // returns false if actor died

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode,
                      const NActors::TActorContext& ctx);

    void SetupCounters();
    void SetupTopicCounters(const TString& topic);
    void SetupTopicCounters(const TString& topic, const TString& cloudId, const TString& dbId,
                            const TString& folderId);

    void ProcessReads(const NActors::TActorContext& ctx); // returns false if actor died
    struct TFormedReadResponse;
    void ProcessAnswer(const NActors::TActorContext& ctx, TIntrusivePtr<TFormedReadResponse> formedResponse); // returns false if actor died

    void RegisterSessions(const NActors::TActorContext& ctx);
    void RegisterSession(const TActorId& pipe, const TString& topic, const TVector<ui32>& groups, const TActorContext& ctx);

    struct TPartitionActorInfo;
    void DropPartition(THashMap<ui64, TPartitionActorInfo>::iterator it, const TActorContext& ctx);

    bool ActualPartitionActor(const TActorId& part);
    void ReleasePartition(const THashMap<ui64, TPartitionActorInfo>::iterator& it,
                        bool couldBeReads, const TActorContext& ctx); // returns false if actor died

    void SendReleaseSignalToClient(const THashMap<ui64, TPartitionActorInfo>::iterator& it, bool kill, const TActorContext& ctx);

    void InformBalancerAboutRelease(const THashMap<ui64, TPartitionActorInfo>::iterator& it, const TActorContext& ctx);

    static ui32 NormalizeMaxReadMessagesCount(ui32 sourceValue);
    static ui32 NormalizeMaxReadSize(ui32 sourceValue);

private:
    std::unique_ptr<NKikimr::NGRpcService::TEvStreamPQReadRequest> Request;

    const TString ClientDC;

    const TInstant StartTimestamp;

    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;
    TIntrusivePtr<NACLib::TUserToken> Token;

    TString ClientId;
    TString ClientPath;
    TString Session;
    TString PeerName;

    bool CommitsDisabled;
    bool BalancersInitStarted;

    bool InitDone;
    bool RangesMode = false;

    ui32 MaxReadMessagesCount;
    ui32 MaxReadSize;
    ui32 MaxTimeLagMs;
    ui64 ReadTimestampMs;

    TString Auth;

    bool ForceACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;

    struct TPartitionActorInfo {
        TActorId Actor;
        const TPartitionId Partition;
        std::deque<ui64> Commits;
        bool Reading;
        bool Releasing;
        bool Released;
        bool LockSent;
        bool ReleaseSent;

        ui64 ReadIdToResponse;
        ui64 ReadIdCommitted;
        TSet<ui64> NextCommits;
        TDisjointIntervalTree<ui64> NextRanges;

        ui64 Offset;

        TInstant AssignTimestamp;

        TPartitionActorInfo(const TActorId& actor, const TPartitionId& partition, const TActorContext& ctx)
            : Actor(actor)
            , Partition(partition)
            , Reading(false)
            , Releasing(false)
            , Released(false)
            , LockSent(false)
            , ReleaseSent(false)
            , ReadIdToResponse(1)
            , ReadIdCommitted(0)
            , Offset(0)
            , AssignTimestamp(ctx.Now())
        { }

        void MakeCommit(const TActorContext& ctx);
    };


    THashSet<TActorId> ActualPartitionActors;
    THashMap<ui64, std::pair<ui32, ui64>> BalancerGeneration;
    ui64 NextAssignId;
    THashMap<ui64, TPartitionActorInfo> Partitions; //assignId -> info

    THashMap<TString, TTopicHolder> Topics; // topic -> info
    THashMap<TString, NPersQueue::TConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashSet<TString> TopicsToResolve;
    THashMap<TString, TVector<ui32>> TopicGroups;
    THashMap<TString, ui64> ReadFromTimestamp;

    bool ReadOnlyLocal;
    TDuration CommitInterval;

    struct TPartitionInfo {
        ui64 AssignId;
        ui64 WTime;
        ui64 SizeLag;
        ui64 MsgLag;
        bool operator < (const TPartitionInfo& rhs) const {
            return std::tie(WTime, AssignId) < std::tie(rhs.WTime, rhs.AssignId);
        }
    };

    TSet<TPartitionInfo> AvailablePartitions;

    struct TFormedReadResponse: public TSimpleRefCount<TFormedReadResponse> {
        using TPtr = TIntrusivePtr<TFormedReadResponse>;

        TFormedReadResponse(const TString& guid, const TInstant start)
            : Guid(guid)
            , Start(start)
            , FromDisk(false)
        {
        }

        PersQueue::V1::MigrationStreamingReadServerMessage Response;
        ui32 RequestsInfly = 0;
        i64 ByteSize = 0;
        ui64 RequestedBytes = 0;

        //returns byteSize diff
        i64 ApplyResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& resp);

        THashSet<TActorId> PartitionsTookPartInRead;
        TSet<TPartitionId> PartitionsTookPartInControlMessages;

        TSet<TPartitionInfo> PartitionsBecameAvailable; // Partitions that became available during this read request execution.

                                                        // These partitions are bringed back to AvailablePartitions after reply to this read request.

        const TString Guid;
        TInstant Start;
        bool FromDisk;
        TDuration WaitQuotaTime;
    };

    THashMap<TActorId, TFormedReadResponse::TPtr> PartitionToReadResponse; // Partition actor -> TFormedReadResponse answer that has this partition.
                                                                           // PartitionsTookPartInRead in formed read response contain this actor id.

    struct TControlMessages {
        TVector<PersQueue::V1::MigrationStreamingReadServerMessage> ControlMessages;
        ui32 Infly = 0;
    };

    TMap<TPartitionId, TControlMessages> PartitionToControlMessages;


    std::deque<THolder<TEvPQProxy::TEvRead>> Reads;

    ui64 Cookie;

    struct TCommitInfo {
        ui64 StartReadId;
        ui32 Partitions;
    };

    TMap<ui64, TCommitInfo> Commits; //readid->TCommitInfo

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    NMonitoring::TDynamicCounters::TCounterPtr SessionsCreated;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsActive;

    NMonitoring::TDynamicCounters::TCounterPtr Errors;
    NMonitoring::TDynamicCounters::TCounterPtr PipeReconnects;
    NMonitoring::TDynamicCounters::TCounterPtr BytesInflight;
    ui64 BytesInflight_;
    ui64 RequestedBytes;
    ui32 ReadsInfly;
    std::queue<ui64> ActiveWrites;

    NKikimr::NPQ::TPercentileCounter PartsPerSession;

    THashMap<TString, TTopicCounters> TopicCounters;
    THashMap<TString, ui32> NumPartitionsFromTopic;

    TVector<NPQ::TLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TPercentileCounter ReadLatency;
    NKikimr::NPQ::TPercentileCounter ReadLatencyFromDisk;
    NKikimr::NPQ::TPercentileCounter CommitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;
    NKikimr::NPQ::TMultiCounter SLIBigReadLatency;
    NKikimr::NPQ::TMultiCounter ReadsTotal;

    NPersQueue::TTopicsListController TopicsHandler;
};


class TReadInfoActor : public TRpcOperationRequestActor<TReadInfoActor, TEvPQReadInfoRequest> {
using TBase = TRpcOperationRequestActor<TReadInfoActor, TEvPQReadInfoRequest>;
public:
     TReadInfoActor(
             NKikimr::NGRpcService::TEvPQReadInfoRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
             const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
             TIntrusivePtr<NMonitoring::TDynamicCounters> counters
     );
    ~TReadInfoActor();

    void Bootstrap(const NActors::TActorContext& ctx);



    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR; }

    bool HasCancelOperation() {
        return false;
    }

private:

    void Die(const NActors::TActorContext& ctx) override;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor
            HFunc(TEvPQProxy::TEvCloseSession, Handle) //from auth actor

            HFunc(TEvPersQueue::TEvResponse, Handle);
        default:
            break;
        };
    }

    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);

    void AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);
    void ProcessAnswers(const TActorContext& ctx);

private:
    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;

    TTopicTabletsPairs TopicAndTablets;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    TString ClientId;
    NPersQueue::TTopicsListController TopicsHandler;
};


class TDropTopicActor : public TPQGrpcSchemaBase<TDropTopicActor, NKikimr::NGRpcService::TEvPQDropTopicRequest> {
using TBase = TPQGrpcSchemaBase<TDropTopicActor, TEvPQDropTopicRequest>;

public:
     TDropTopicActor(NKikimr::NGRpcService::TEvPQDropTopicRequest* request);
    ~TDropTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }
};

class TDescribeTopicActor : public TPQGrpcSchemaBase<TDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                          , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
};


class TAddReadRuleActor : public TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>
                        , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>;

public:
    TAddReadRuleActor(NKikimr::NGRpcService::TEvPQAddReadRuleRequest *request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};

class TRemoveReadRuleActor : public TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>
                           , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>;

public:
    TRemoveReadRuleActor(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest* request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};


class TCreateTopicActor : public TPQGrpcSchemaBase<TCreateTopicActor, NKikimr::NGRpcService::TEvPQCreateTopicRequest> {
using TBase = TPQGrpcSchemaBase<TCreateTopicActor, TEvPQCreateTopicRequest>;

public:
     TCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters);
    ~TCreateTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
    TVector<TString> Clusters;
};

class TAlterTopicActor : public TPQGrpcSchemaBase<TAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest> {
using TBase = TPQGrpcSchemaBase<TAlterTopicActor, TEvPQAlterTopicRequest>;

public:
     TAlterTopicActor(NKikimr::NGRpcService::TEvPQAlterTopicRequest* request);
    ~TAlterTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }
};



}
