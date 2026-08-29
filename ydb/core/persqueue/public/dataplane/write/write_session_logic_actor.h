#pragma once

#include "write_request_info.h"
#include "write_session_events.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/counters/percentile_counter.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/generic/deque.h>

#include <optional>

namespace NKikimr::NPQ::NDataplane::NWrite {

class TWriteSessionLogicActor
    : public TBaseActor<TWriteSessionLogicActor>
    , public TConstantLogPrefix
    , private TRlHelpers
{
    using TBase = TBaseActor<TWriteSessionLogicActor>;

public:
    explicit TWriteSessionLogicActor(TWriteSessionSettings settings);

    void Bootstrap();
    void PassAway() override;
    void OnException(const std::exception& exc) override;
    TString BuildLogPrefix() const override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_PQ_WRITE;
    }

private:
    STFUNC(StateFunc);

    void Handle(TEvents::TEvPoison::TPtr& ev);
    void Handle(TEvInit::TPtr& ev);
    void Handle(TEvWrite::TPtr& ev);
    void Handle(TEvUpdateToken::TPtr& ev);
    void Handle(TEvTokenRefreshed::TPtr& ev);
    void Handle(TEvClientDone::TPtr& ev);
    void Handle(TEvDieCommand::TPtr& ev);

    void OnWriteAccessGranted();
    void RecheckACL();
    void InitCheckSchema(bool needWaitSchema = false, NWilson::TTraceId traceId = {});
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev);
    void LogSession();
    TString TopicLogName() const;
    bool InitAfterDiscovery();

    void ProceedPartition(ui32 partition);
    void Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev);
    void MakeAndSendInitResponse(const std::optional<ui64>& maxSeqNo);
    void Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr& ev);
    void ProcessWriteResponse(const NKikimrClient::TPersQueuePartitionResponse& response);
    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev);
    void Handle(TEvPartitionWriter::TEvDisconnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);

    void DiscoverPartition();
    void Handle(TEvPartitionChooser::TEvChooseResult::TPtr& ev);
    void Handle(TEvPartitionChooser::TEvChooseError::TPtr& ev);

    void Handle(TEvents::TEvWakeup::TPtr& ev);

    void CloseSession(const TString& errorReason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                      std::optional<Ydb::StatusIds::StatusCode> statusOverride = std::nullopt);
    void CompleteSession(const TString& errorReason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                         std::optional<Ydb::StatusIds::StatusCode> statusOverride = std::nullopt);
    void CheckFinish();

    void PrepareRequest(THolder<TEvWrite>&& ev);
    void SendWriteRequest(TWriteRequestInfo::TPtr&& request);

    void SetupBytesWrittenByUserAgentCounter(const TString& topicPath);
    void SetupCounters();
    void SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath, bool isServerless, const TString& folderId);

    void CloseSpans(const TString& errorReason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode);

    bool CreatePartitionWriterCache();
    void DestroyPartitionWriterCache();

    NWilson::TSpan GenerateSpan(NJaegerTracing::ERequestType subrequestType, TStringBuf name) const;
    NWilson::TSpan GenerateInitSpan() const;
    NWilson::TSpan GenerateWriteSpan() const;
    NWilson::TSpan GenerateUpdateTokenSpan() const;

    using TRlHelpers::MaybeRequestQuota;
    void MaybeRequestQuota(EWakeupTag tag);

    enum EState {
        ES_CREATED = 1,
        ES_WAIT_SCHEME = 2,
        ES_WAIT_PARTITION = 3,
        ES_WAIT_WRITER_INIT = 7,
        ES_INITED = 8,
        ES_DYING = 9
    };

    NActors::TActorId Owner;
    TWriteSessionProtocolOpts Protocol;
    TString UserAgent;
    TString SdkBuildInfo;
    std::optional<TString> DatabaseName;
    TString SerializedToken;
    std::optional<TString> TraceId;
    std::optional<TString> RequestType;
    NWilson::TTraceId WilsonTraceId;

    EState State;

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
    std::optional<bool> TrackProducerId;

    TString SourceId;
    bool UseDeduplication = true;

    TString OwnerCookie;

    TIntrusivePtr<TSecurityObject> ACL;

    std::deque<TWriteRequestInfo::TPtr> PendingRequests;
    TWriteRequestInfo::TPtr PendingQuotaRequest;
    std::deque<TWriteRequestInfo::TPtr> SentRequests;
    std::deque<TWriteRequestInfo::TPtr> AcceptedRequests;

    bool WritesDone;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TMultiCounter BytesInflight;
    TMultiCounter BytesInflightTotal;

    ui64 BytesInflight_;
    ui64 BytesInflightTotal_;

    bool NextRequestInited;
    ui64 NextRequestCookie;

    TMultiCounter SessionsCreated;
    TMultiCounter SessionsActive;

    TMultiCounter Errors;
    std::vector<TMultiCounter> CodecCounters;

    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWrittenByUserAgent;

    TIntrusiveConstPtr<NACLib::TUserToken> Token;
    TString Auth;
    bool UpdateTokenInProgress;
    bool UpdateTokenAuthenticated;
    bool ACLCheckInProgress;
    bool FirstACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;
    TInstant LogSessionDeadline;

    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
    NKikimrPQ::TPQTabletConfig InitialPQTabletConfig;
    std::shared_ptr<IPartitionChooser> Chooser;
    std::shared_ptr<TPartitionGraph> PartitionGraph;

    NKikimrPQClient::TDataChunk InitMeta;
    TString ClientDC;
    THashMap<TString, TString> SessionMeta;

    TInstant LastSourceIdUpdate;

    THashMap<ui64, TString> DeferredPublicationExtByInt;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    TMultiCounter SLITotal;
    TMultiCounter SLIErrors;
    TInstant StartTime;
    TPercentileCounter InitLatency;
    TMultiCounter SLIBigLatency;

    TString TopicPath;
    TString DescribedRealPath;

    TActorId PartitionWriterCache;
    TActorId PartitionChooser;
    TActorId Describer;

    bool SessionClosed = false;
    NWilson::TSpan Span;
    NWilson::TSpan InitSpan;
    NWilson::TSpan UpdateTokenSpan;
};

} // namespace NKikimr::NPQ::NDataplane::NWrite
