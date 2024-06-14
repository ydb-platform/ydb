#pragma once

#include "grpc_pq_session.h"
#include "ydb/core/client/server/msgbus_server_pq_metacache.h"
#include "ydb/core/client/server/msgbus_server_persqueue.h"

#include <ydb/core/base/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/deprecated/persqueue_v0/api/grpc/persqueue.grpc.pb.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/core/kqp/common/kqp.h>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/services/lib/actors/type_definitions.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>
#include <ydb/services/persqueue_v1/actors/read_session_actor.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>
#include <ydb/services/metadata/service.h>

#include <util/generic/guid.h>
#include <util/system/compiler.h>

namespace NKikimr {
namespace NGRpcProxy {





static inline bool InternalErrorCode(NPersQueue::NErrorCode::EErrorCode errorCode) {
    switch(errorCode) {
        case NPersQueue::NErrorCode::UNKNOWN_TOPIC:
        case NPersQueue::NErrorCode::ERROR:
        case NPersQueue::NErrorCode::INITIALIZING:
        case NPersQueue::NErrorCode::OVERLOAD:
        case NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL:
            return true;
        default:
            return false;
    }
    return false;
}



Ydb::StatusIds::StatusCode ConvertPersQueueInternalCodeToStatus(const NPersQueue::NErrorCode::EErrorCode code);
void FillIssue(Ydb::Issue::IssueMessage* issue, const NPersQueue::NErrorCode::EErrorCode errorCode, const TString& errorReason);

using IWriteSessionHandlerRef = TIntrusivePtr<ISessionHandler<NPersQueue::TWriteResponse>>;
using IReadSessionHandlerRef = TIntrusivePtr<ISessionHandler<NPersQueue::TReadResponse>>;

const TString& LocalDCPrefix();
const TString& MirroredDCPrefix();

constexpr ui64 MAGIC_COOKIE_VALUE = 123456789;

static const TDuration CHECK_ACL_DELAY = TDuration::Minutes(5);

struct TEvPQProxy {
    enum EEv {
        EvWriteInit  = EventSpaceBegin(TKikimrEvents::ES_PQ_PROXY),
        EvWrite,
        EvDone,
        EvReadInit,
        EvRead,
        EvCloseSession,
        EvPartitionReady,
        EvReadResponse,
        EvCommit,
        EvCommitDone,
        EvLocked,
        EvReleasePartition,
        EvPartitionReleased,
        EvLockPartition,
        EvRestartPipe,
        EvDieCommand,
        EvPartitionStatus,
        EvAuth,
        EvReadSessionStatus,
        EvReadSessionStatusResponse,
        EvDeadlineExceeded,
        EvGetStatus,
        EvWriteDone,
        EvMoveTopic,
        EvReadingStarted,
        EvReadingFinished,
        EvEnd,
    };

    struct TEvReadSessionStatus : public TEventPB<TEvReadSessionStatus, NKikimrPQ::TReadSessionStatus, EvReadSessionStatus> {
    };

    struct TEvReadSessionStatusResponse : public TEventPB<TEvReadSessionStatusResponse, NKikimrPQ::TReadSessionStatusResponse, EvReadSessionStatusResponse> {
    };



    struct TEvWriteInit : public NActors::TEventLocal<TEvWriteInit, EvWriteInit> {
        TEvWriteInit(const NPersQueue::TWriteRequest& req, const TString& peerName, const TString& database)
            : Request(req)
            , PeerName(peerName)
            , Database(database)
        { }

        NPersQueue::TWriteRequest Request;
        TString PeerName;
        TString Database;
    };

    struct TEvWrite : public NActors::TEventLocal<TEvWrite, EvWrite> {
        explicit TEvWrite(const NPersQueue::TWriteRequest& req)
            : Request(req)
        { }

        NPersQueue::TWriteRequest Request;
    };

    struct TEvDone : public NActors::TEventLocal<TEvDone, EvDone> {
        TEvDone()
        { }
    };

    struct TEvWriteDone : public NActors::TEventLocal<TEvWriteDone, EvWriteDone> {
        TEvWriteDone(ui64 size)
            : Size(size)
        { }

        ui64 Size;
    };

    struct TEvReadInit : public NActors::TEventLocal<TEvReadInit, EvReadInit> {
        TEvReadInit(const NPersQueue::TReadRequest& req, const TString& peerName, const TString& database)
            : Request(req)
            , PeerName(peerName)
            , Database(database)
        { }

        NPersQueue::TReadRequest Request;
        TString PeerName;
        TString Database;
    };

    struct TEvRead : public NActors::TEventLocal<TEvRead, EvRead> {
        explicit TEvRead(const NPersQueue::TReadRequest& req, const TString& guid = CreateGuidAsString())
            : Request(req)
            , Guid(guid)
        { }

        NPersQueue::TReadRequest Request;
        const TString Guid;
    };
    struct TEvCloseSession : public NActors::TEventLocal<TEvCloseSession, EvCloseSession> {
        TEvCloseSession(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode)
            : Reason(reason)
        , ErrorCode(errorCode)
        { }

        const TString Reason;
        NPersQueue::NErrorCode::EErrorCode ErrorCode;
    };

    struct TEvPartitionReady : public NActors::TEventLocal<TEvPartitionReady, EvPartitionReady> {
        TEvPartitionReady(const NPersQueue::TTopicConverterPtr& topic, const ui32 partition, const ui64 wTime, const ui64 sizeLag,
                          const ui64 readOffset, const ui64 endOffset)
            : Topic(topic)
            , Partition(partition)
            , WTime(wTime)
            , SizeLag(sizeLag)
            , ReadOffset(readOffset)
            , EndOffset(endOffset)
        { }

        NPersQueue::TTopicConverterPtr Topic;
        ui32 Partition;
        ui64 WTime;
        ui64 SizeLag;
        ui64 ReadOffset;
        ui64 EndOffset;
    };

    struct TEvReadResponse : public NActors::TEventLocal<TEvReadResponse, EvReadResponse> {
        explicit TEvReadResponse(
            NPersQueue::TReadResponse&& resp,
            ui64 nextReadOffset,
            bool fromDisk,
            TDuration waitQuotaTime
        )
            : Response(std::move(resp))
            , NextReadOffset(nextReadOffset)
            , FromDisk(fromDisk)
            , WaitQuotaTime(waitQuotaTime)
        { }

        NPersQueue::TReadResponse Response;
        ui64 NextReadOffset;
        bool FromDisk;
        TDuration WaitQuotaTime;
    };

    struct TEvCommit : public NActors::TEventLocal<TEvCommit, EvCommit> {
        explicit TEvCommit(ui64 readId, ui64 offset = Max<ui64>())
            : ReadId(readId)
            , Offset(offset)
        { }

        ui64 ReadId;
        ui64 Offset; // Actual value for requests to concreete partitions
    };

    struct TEvAuth : public NActors::TEventLocal<TEvAuth, EvAuth> {
        TEvAuth(const NPersQueueCommon::TCredentials& auth)
            : Auth(auth)
        { }

        NPersQueueCommon::TCredentials Auth;
    };

    struct TEvLocked : public NActors::TEventLocal<TEvLocked, EvLocked> {
        TEvLocked(const TString& topic, ui32 partition, ui64 readOffset, ui64 commitOffset, bool verifyReadOffset, ui64 generation)
            : Topic(topic)
            , Partition(partition)
            , ReadOffset(readOffset)
            , CommitOffset(commitOffset)
            , VerifyReadOffset(verifyReadOffset)
            , Generation(generation)
        { }

        TString Topic;
        ui32 Partition;
        ui64 ReadOffset;
        ui64 CommitOffset;
        bool VerifyReadOffset;
        ui64 Generation;
    };

    struct TEvGetStatus : public NActors::TEventLocal<TEvGetStatus, EvGetStatus> {
        TEvGetStatus(const TString& topic, ui32 partition, ui64 generation)
            : Topic(topic)
            , Partition(partition)
            , Generation(generation)
        { }

        TString Topic;
        ui32 Partition;
        ui64 Generation;
    };



    struct TEvCommitDone : public NActors::TEventLocal<TEvCommitDone, EvCommitDone> {
        TEvCommitDone(ui64 readId, const NPersQueue::TTopicConverterPtr& topic, const ui32 partition)
            : ReadId(readId)
            , Topic(topic)
            , Partition(partition)
        { }

        ui64 ReadId;
        NPersQueue::TTopicConverterPtr Topic;
        ui32 Partition;
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
        TEvPartitionReleased(const NPersQueue::TTopicConverterPtr& topic, const ui32 partition)
            : Topic(topic)
            , Partition(partition)
        { }

        NPersQueue::TTopicConverterPtr Topic;
        ui32 Partition;
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
        TEvDieCommand(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode)
        : Reason(reason)
        , ErrorCode(errorCode)
        { }

        TString Reason;
        NPersQueue::NErrorCode::EErrorCode ErrorCode;
    };

    struct TEvPartitionStatus : public NActors::TEventLocal<TEvPartitionStatus, EvPartitionStatus> {
        TEvPartitionStatus(const NPersQueue::TTopicConverterPtr& topic, const ui32 partition, const ui64 offset,
                           const ui64 endOffset, ui64 writeTimestampEstimateMs, bool init = true)
        : Topic(topic)
        , Partition(partition)
        , Offset(offset)
        , EndOffset(endOffset)
        , WriteTimestampEstimateMs(writeTimestampEstimateMs)
        , Init(init)
        { }

        NPersQueue::TTopicConverterPtr Topic;
        ui32 Partition;
        ui64 Offset;
        ui64 EndOffset;
        ui64 WriteTimestampEstimateMs;
        bool Init;
    };


    struct TEvMoveTopic : public NActors::TEventLocal<TEvMoveTopic, EvMoveTopic> {
        TEvMoveTopic(const TString& sourcePath, const TString& destinationPath)
            : SourcePath(sourcePath)
            , DestinationPath(destinationPath)
        { }

        TString SourcePath;
        TString DestinationPath;
    };

    struct TEvReadingStarted : public TEventLocal<TEvReadingStarted, EvReadingStarted> {
        TEvReadingStarted(const TString& topic, ui32 partitionId)
            : Topic(topic)
            , PartitionId(partitionId)
        {}

        TString Topic;
        ui32 PartitionId;
    };

    struct TEvReadingFinished : public TEventLocal<TEvReadingFinished, EvReadingFinished> {
        TEvReadingFinished(const TString& topic, ui32 partitionId, bool first)
            : Topic(topic)
            , PartitionId(partitionId)
            , FirstMessage(first)
        {}

        TString Topic;
        ui32 PartitionId;
        bool FirstMessage;
    };
};



/// WRITE ACTOR
class TWriteSessionActor : public NActors::TActorBootstrapped<TWriteSessionActor> {
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;
    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TPQGroupInfoPtr = TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>;
public:
    TWriteSessionActor(IWriteSessionHandlerRef handler, const ui64 cookie, const NActors::TActorId& schemeCache,
                       TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TString& localDC,
                       const TMaybe<TString> clientDC);
    ~TWriteSessionActor();

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_WRITE; }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)

            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvPQProxy::TEvWriteInit,  Handle)
            HFunc(TEvPQProxy::TEvWrite, Handle)
            HFunc(TEvPQProxy::TEvDone, Handle)

            HFunc(TEvDescribeTopicsResponse, Handle);

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

    void DiscoverPartition(const NActors::TActorContext& ctx);
    void Handle(NPQ::TEvPartitionChooser::TEvChooseResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NPQ::TEvPartitionChooser::TEvChooseError::TPtr& ev, const NActors::TActorContext& ctx);

    TString CheckSupportedCodec(const ui32 codecId);
    void CheckACL(const TActorContext& ctx);
    void InitCheckACL(const TActorContext& ctx);
    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvWriteInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvWrite::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);

    void LogSession(const TActorContext& ctx);

    void InitAfterDiscovery(const TActorContext& ctx);
    void ProceedPartition(const ui32 partition, const NActors::TActorContext& ctx);

    void Handle(TEvDescribeTopicsResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);

    void CloseSession(const TString& errorReason, const NPersQueue::NErrorCode::EErrorCode errorCode, const NActors::TActorContext& ctx);

    void CheckFinish(const NActors::TActorContext& ctx);

    void GenerateNextWriteRequest(const NActors::TActorContext& ctx);

    void SetupCounters();
    void SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath,
                       bool isServerless, const TString& folderId);


private:
    IWriteSessionHandlerRef Handler;

    enum EState {
        ES_CREATED = 1,
        ES_WAIT_SCHEME = 2,
        ES_WAIT_PARTITION = 3,
        ES_WAIT_WRITER_INIT = 7,
        ES_INITED = 8,
        ES_DYING = 9,
    };

    EState State;
    TActorId SchemeCache;
    TActorId Writer;

    TString PeerName;
    TString Database;
    ui64 Cookie;

    ui32 Partition;
    ui64 PartitionTabletId;
    ui32 PreferedPartition;
    TString SourceId;
    std::optional<ui64> InitialSeqNo;
    TString OwnerCookie;
    TString UserAgent;

    ui32 NumReserveBytesRequests;

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

    NKikimrSchemeOp::TPersQueueGroupDescription Config;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    NKikimr::NPQ::TMultiCounter BytesInflight;
    NKikimr::NPQ::TMultiCounter BytesInflightTotal;

    ui64 BytesInflight_;
    ui64 BytesInflightTotal_;

    bool NextRequestInited;

    NKikimr::NPQ::TMultiCounter SessionsCreated;
    NKikimr::NPQ::TMultiCounter SessionsActive;
    NKikimr::NPQ::TMultiCounter SessionsWithoutAuth;

    NKikimr::NPQ::TMultiCounter Errors;
    std::vector<NKikimr::NPQ::TMultiCounter> CodecCounters;
    ui64 NextRequestCookie;

    TIntrusiveConstPtr<NACLib::TUserToken> Token;
    NPersQueueCommon::TCredentials Auth;
    TString AuthStr;
    bool ACLCheckInProgress;
    bool FirstACLCheck;
    bool ForceACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;
    TInstant LogSessionDeadline;

    TString DatabaseId;
    TString FolderId;
    TIntrusivePtr<TSecurityObject> SecurityObject;
    TPQGroupInfoPtr PQInfo;

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

    THolder<NPersQueue::TTopicNamesConverterFactory> ConverterFactory;
    NPersQueue::TDiscoveryConverterPtr DiscoveryConverter;
    NPersQueue::TTopicConverterPtr FullConverter;

    NPersQueue::TWriteRequest::TInit InitRequest;

    TActorId PartitionChooser;

    bool SessionClosed = false;
};

class TReadSessionActor : public TActorBootstrapped<TReadSessionActor> {
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;
    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
public:
     TReadSessionActor(IReadSessionHandlerRef handler, const NPersQueue::TTopicsListController& topicsHandler, const ui64 cookie,
                        const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                        const TMaybe<TString> clientDC);
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
    };

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)

            HFunc(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvPQProxy::TEvReadInit,  Handle) //from gRPC
            HFunc(TEvPQProxy::TEvReadSessionStatus, Handle) // from read sessions info builder proxy
            HFunc(TEvPQProxy::TEvRead, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvDone, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvWriteDone, Handle) //from gRPC
            HFunc(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvCloseSession, Handle) //from partitionActor

            HFunc(TEvPQProxy::TEvPartitionReady, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReleased, Handle) //from partitionActor

            HFunc(TEvPQProxy::TEvReadingStarted, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvReadingFinished, Handle); // from partitionActor

            HFunc(TEvPQProxy::TEvReadResponse, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvCommit, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvLocked, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvGetStatus, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvAuth, Handle) //from gRPC

            HFunc(TEvPQProxy::TEvCommitDone, Handle) //from PartitionActor
            HFunc(TEvPQProxy::TEvPartitionStatus, Handle) //from partitionActor

            HFunc(TEvPersQueue::TEvLockPartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvReleasePartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvError, Handle) //from Balancer

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

            HFunc(TEvDescribeTopicsResponse, HandleDescribeTopicsResponse);
            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);

        default:
            break;
        };
    }

    void Handle(TEvPQProxy::TEvReadInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvWriteDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommit::TPtr& ev, const NActors::TActorContext& ctx);
    void MakeCommit(const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvLocked::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuth::TPtr& ev, const NActors::TActorContext& ctx);
    void ProcessAuth(const NPersQueueCommon::TCredentials& auth);
    void Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const NActors::TActorContext& ctx);
    void AnswerForCommitsIfCan(const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvError::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvReadingStarted::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadingFinished::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    [[nodiscard]] bool ProcessBalancerDead(const ui64 tabletId, const NActors::TActorContext& ctx); // returns false if actor died

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);
    void Handle(NKikimr::NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void CloseSession(const TString& errorReason, const NPersQueue::NErrorCode::EErrorCode errorCode,
                      const NActors::TActorContext& ctx);

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx);
    void HandleDescribeTopicsResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx);

    void SendAuthRequest(const TActorContext& ctx);
    void CreateInitAndAuthActor(const TActorContext& ctx);

    void SetupCounters();
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic);
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic, const TString& cloudId, const TString& dbId,
                            const TString& dbPath, bool isServerless, const TString& folderId);

    [[nodiscard]] bool ProcessReads(const NActors::TActorContext& ctx); // returns false if actor died
    struct TFormedReadResponse;
    [[nodiscard]] bool ProcessAnswer(const NActors::TActorContext& ctx, TIntrusivePtr<TFormedReadResponse> formedResponse); // returns false if actor died

    void RegisterSessions(const NActors::TActorContext& ctx);
    void RegisterSession(const TActorId& pipe, const TString& topic, const TActorContext& ctx);

    struct TPartitionActorInfo;
    void DropPartitionIfNeeded(THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator it, const TActorContext& ctx);

    bool ActualPartitionActor(const TActorId& part);
    [[nodiscard]] bool ProcessReleasePartition(const THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator& it,
                        bool kill, bool couldBeReads, const TActorContext& ctx); // returns false if actor died
    void InformBalancerAboutRelease(const THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator& it, const TActorContext& ctx);

    // returns false if check failed.
    bool CheckAndUpdateReadSettings(const NPersQueue::TReadRequest::TRead& readRequest);

    static ui32 NormalizeMaxReadMessagesCount(ui32 sourceValue);
    static ui32 NormalizeMaxReadSize(ui32 sourceValue);
    static ui32 NormalizeMaxReadPartitionsCount(ui32 sourceValue);

    static bool RemoveEmptyMessages(NPersQueue::TReadResponse::TBatchedData& data); // returns true if there are nonempty messages

private:
    IReadSessionHandlerRef Handler;

    const TInstant StartTimestamp;

    TActorId PqMetaCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;
    bool AuthInflight;

    TString InternalClientId;
    TString ExternalClientId;
    const TString ClientDC;
    TString ClientPath;
    TString Session;
    TString PeerName;
    TString Database;

    bool ClientsideLocksAllowed;
    bool BalanceRightNow;
    bool CommitsDisabled;
    bool BalancersInitStarted;

    bool InitDone;

    ui32 ProtocolVersion; // from NPersQueue::TReadRequest::EProtocolVersion
    // Read settings.
    // Can be initialized during Init request (new preferable way)
    // or during read request (old way that will be removed in future).
    // These settings can't be changed (in that case server closes session).
    ui32 MaxReadMessagesCount;
    ui32 MaxReadSize;
    ui32 MaxReadPartitionsCount;
    ui32 MaxTimeLagMs;
    ui64 ReadTimestampMs;
    bool ReadSettingsInited;

    NPersQueueCommon::TCredentials Auth;
    TString AuthStr;
    TIntrusiveConstPtr<NACLib::TUserToken> Token;
    bool ForceACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;

    struct TPartitionActorInfo {
        TActorId Actor;
        std::deque<ui64> Commits;
        bool Reading;
        bool Releasing;
        bool Released;
        ui64 LockGeneration;
        bool LockSent;
        NPersQueue::TTopicConverterPtr Converter;

        TPartitionActorInfo(const TActorId& actor, ui64 generation, const NPersQueue::TTopicConverterPtr& topic)
            : Actor(actor)
            , Reading(false)
            , Releasing(false)
            , Released(false)
            , LockGeneration(generation)
            , LockSent(false)
            , Converter(topic)
        {}
    };


    THashSet<TActorId> ActualPartitionActors;
    THashMap<std::pair<TString, ui32>, TPartitionActorInfo> Partitions; //topic[ClientSideName!]:partition -> info

    THashMap<TString, NPersQueue::TTopicConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashMap<TString, TTopicHolder> Topics; // PrimaryName ->topic info

    TVector<ui32> Groups;
    bool ReadOnlyLocal;

    struct TPartitionInfo {
        NPersQueue::TTopicConverterPtr Topic;
        ui32 Partition;
        ui64 WTime;
        ui64 SizeLag;
        ui64 MsgLag;
        TActorId Actor;
        bool operator < (const TPartitionInfo& rhs) const {
            return std::tie(WTime, Topic, Partition, Actor) < std::tie(rhs.WTime, rhs.Topic, rhs.Partition, rhs.Actor);
        }
    };

    TSet<TPartitionInfo> AvailablePartitions;

    struct TOffsetsInfo {
        struct TPartitionOffsetInfo {
            TPartitionOffsetInfo(const TActorId& sender, const TString& topic, ui32 partition, ui64 offset)
                : Sender(sender)
                , Topic(topic)
                , Partition(partition)
                , Offset(offset)
            {
            }

            TActorId Sender;
            TString Topic;
            ui32 Partition;
            ui64 Offset;
        };

        // find by read id
        bool operator<(ui64 readId) const {
            return ReadId < readId;
        }

        friend bool operator<(ui64 readId, const TOffsetsInfo& info) {
            return readId < info.ReadId;
        }

        ui64 ReadId = 0;
        std::vector<TPartitionOffsetInfo> PartitionOffsets;
    };

    std::deque<TOffsetsInfo> Offsets; // Sequential read id -> offsets

    struct TFormedReadResponse: public TSimpleRefCount<TFormedReadResponse> {
        using TPtr = TIntrusivePtr<TFormedReadResponse>;

        TFormedReadResponse(const TString& guid, const TInstant start)
            : Guid(guid)
            , Start(start)
            , FromDisk(false)
        {
        }

        NPersQueue::TReadResponse Response;
        ui32 RequestsInfly = 0;
        i64 ByteSize = 0;

        ui64 RequestedBytes = 0;

        //returns byteSize diff
        i64 ApplyResponse(NPersQueue::TReadResponse&& resp);

        TVector<NPersQueue::TReadResponse> ControlMessages;

        THashSet<TActorId> PartitionsTookPartInRead;
        TSet<TPartitionInfo> PartitionsBecameAvailable; // Partitions that became available during this read request execution.
                                                        // These partitions are bringed back to AvailablePartitions after reply to this read request.
        TOffsetsInfo Offsets; // Offsets without assigned read id.

        const TString Guid;
        TInstant Start;
        bool FromDisk;
        TDuration WaitQuotaTime;
    };

    THashMap<TActorId, TFormedReadResponse::TPtr> PartitionToReadResponse; // Partition actor -> TFormedReadResponse answer that has this partition.
                                                                           // PartitionsTookPartInRead in formed read response contain this actor id.

    ui64 ReadIdToResponse;
    ui64 ReadIdCommitted;
    TSet<ui64> NextCommits;
    TInstant LastCommitTimestamp;
    TDuration CommitInterval;
    ui32 CommitsInfly;

    std::deque<THolder<TEvPQProxy::TEvRead>> Reads;

    ui64 Cookie;

    struct TCommitInfo {
        ui64 StartReadId;
        ui32 Partitions;
        TInstant StartTime;
    };

    TMap<ui64, TCommitInfo> Commits; //readid->TCommitInfo

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    NMonitoring::TDynamicCounters::TCounterPtr SessionsCreated;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsActive;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsWithoutAuth;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsWithOldBatchingVersion; // LOGBROKER-3173

    NMonitoring::TDynamicCounters::TCounterPtr Errors;
    NMonitoring::TDynamicCounters::TCounterPtr PipeReconnects;
    NMonitoring::TDynamicCounters::TCounterPtr BytesInflight;
    ui64 BytesInflight_;
    ui64 RequestedBytes;
    ui32 ReadsInfly;

    NKikimr::NPQ::TPercentileCounter PartsPerSession;

    THashMap<TString, TTopicCounters> TopicCounters;
    THashMap<TString, ui32> NumPartitionsFromTopic;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TPercentileCounter CommitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;

    NKikimr::NPQ::TPercentileCounter ReadLatency;
    NKikimr::NPQ::TPercentileCounter ReadLatencyFromDisk;
    NKikimr::NPQ::TMultiCounter SLIBigReadLatency;
    NKikimr::NPQ::TMultiCounter ReadsTotal;

    NPersQueue::TTopicsListController TopicsHandler;
    NPersQueue::TTopicsToConverter TopicsList;
};

}
}
