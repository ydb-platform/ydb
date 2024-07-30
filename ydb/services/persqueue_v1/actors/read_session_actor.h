#pragma once

#include "events.h"
#include "partition_actor.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/generic/guid.h>
#include <util/system/compiler.h>

#include <google/protobuf/util/time_util.h>

#include <type_traits>

namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQReadServiceActorID() {
    return TActorId(0, "PQReadSvc");
}

struct TPartitionActorInfo {
    const TActorId Actor;
    const TPartitionId Partition;
    NPersQueue::TTopicConverterPtr Topic;
    std::deque<ui64> Commits;
    bool Reading;
    bool Releasing;
    bool Stopping;
    bool Released;
    bool LockSent;
    bool ReleaseSent;

    ui64 ReadIdToResponse;
    ui64 ReadIdCommitted;
    TSet<ui64> NextCommits;
    TDisjointIntervalTree<ui64> NextRanges;
    ui64 Offset;

    TInstant AssignTimestamp;

    ui64 Generation;
    ui64 NodeId;


    struct TDirectReadInfo {
        ui64 DirectReadId = 0;
        ui64 ByteSize = 0;
    };

    ui64 MaxProcessedDirectReadId = 0;
    ui64 LastDirectReadId = 0;

    std::map<i64, TDirectReadInfo> DirectReads;

    explicit TPartitionActorInfo(
            const TActorId& actor,
            const TPartitionId& partition,
            const NPersQueue::TTopicConverterPtr& topic,
            const TInstant& timestamp)
        : Actor(actor)
        , Partition(partition)
        , Topic(topic)
        , Reading(false)
        , Releasing(false)
        , Stopping(false)
        , Released(false)
        , LockSent(false)
        , ReleaseSent(false)
        , ReadIdToResponse(1)
        , ReadIdCommitted(0)
        , Offset(0)
        , AssignTimestamp(timestamp)
        , Generation(0)
        , NodeId(0)
    {
        Y_ABORT_UNLESS(partition.DiscoveryConverter != nullptr);
    }
};

struct TPartitionInfo {
    ui64 AssignId;
    ui64 WTime;
    ui64 SizeLag;
    ui64 MsgLag;

    explicit TPartitionInfo(ui64 assignId, ui64 wTime, ui64 sizeLag, ui64 msgLag)
        : AssignId(assignId)
        , WTime(wTime)
        , SizeLag(sizeLag)
        , MsgLag(msgLag)
    {
    }

    bool operator < (const TPartitionInfo& rhs) const {
        return std::tie(WTime, AssignId) < std::tie(rhs.WTime, rhs.AssignId);
    }
};

template <typename TServerMessage>
struct TFormedReadResponse: public TSimpleRefCount<TFormedReadResponse<TServerMessage>> {
    using TPtr = TIntrusivePtr<TFormedReadResponse>;

    TFormedReadResponse(const TString& guid, const TInstant start)
        : Guid(guid)
        , Start(start)
        , FromDisk(false)
    {
    }

    TServerMessage Response;
    ui32 RequestsInfly = 0;
    i64 ByteSize = 0;
    ui64 RequestedBytes = 0;

    bool HasMessages = false;
    i64 ByteSizeBeforeFiltering = 0;
    ui64 RequiredQuota = 0;

    bool IsDirectRead = false;
    ui64 AssignId = 0;
    ui64 DirectReadId = 0;
    ui64 DirectReadByteSize = 0;

    // returns byteSize diff
    i64 ApplyResponse(TServerMessage&& resp);

    i64 ApplyDirectReadResponse(TEvPQProxy::TEvDirectReadResponse::TPtr& ev);

    THashSet<TActorId> PartitionsTookPartInRead;
    TSet<TPartitionId> PartitionsTookPartInControlMessages;

    // Partitions that became available during this read request execution.
    // These partitions are bringed back to AvailablePartitions after reply to this read request.
    TSet<TPartitionInfo> PartitionsBecameAvailable;

    const TString Guid;
    TInstant Start;
    bool FromDisk;
    TDuration WaitQuotaTime;
};

template <bool UseMigrationProtocol> // Migration protocol is "pqv1"
class TReadSessionActor
    : public TActorBootstrapped<TReadSessionActor<UseMigrationProtocol>>
    , private NPQ::TRlHelpers
{
    using TClientMessage = typename std::conditional_t<UseMigrationProtocol,
        PersQueue::V1::MigrationStreamingReadClientMessage,
        Topic::StreamReadMessage::FromClient>;

    using TServerMessage = typename std::conditional_t<UseMigrationProtocol,
        PersQueue::V1::MigrationStreamingReadServerMessage,
        Topic::StreamReadMessage::FromServer>;

    using TEvReadInit = typename std::conditional_t<UseMigrationProtocol,
        TEvPQProxy::TEvMigrationReadInit,
        TEvPQProxy::TEvReadInit>;

    using TEvReadResponse = typename std::conditional_t<UseMigrationProtocol,
        TEvPQProxy::TEvMigrationReadResponse,
        TEvPQProxy::TEvReadResponse>;

    using TEvStreamReadRequest = typename std::conditional_t<UseMigrationProtocol,
        NGRpcService::TEvStreamPQMigrationReadRequest,
        NGRpcService::TEvStreamTopicReadRequest>;

    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

    using TPartitionsMap = std::unordered_map<ui64, TPartitionActorInfo>;
    using TPartitionsMapIterator = typename TPartitionsMap::iterator;

private:
    // 11 tries = 10,23 seconds, then each try for 5 seconds , so 21 retries will take near 1 min
    static constexpr NTabletPipe::TClientRetryPolicy RetryPolicyForPipes = {
        .RetryLimitCount = 21,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::Seconds(5),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };

    static constexpr ui64 MAX_INFLY_BYTES = 25_MB;
    static constexpr ui32 MAX_INFLY_READS = 10;

    static constexpr ui64 MAX_READ_SIZE = 100_MB;
    static constexpr ui64 READ_BLOCK_SIZE = 8_KB; // metering

    static constexpr double LAG_GROW_MULTIPLIER = 1.2; // assume that 20% more data arrived to partitions

public:
    TReadSessionActor(TEvStreamReadRequest* request, const ui64 cookie,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler);

    void Bootstrap(const TActorContext& ctx);

    void Die(const TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_PQ_READ;
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            // grpc events
            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            HFunc(IContext::TEvNotifiedWhenDone, Handle)
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            // proxy events
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor
            HFunc(/* type alias */ TEvReadInit,  Handle); // from gRPC
            HFunc(TEvPQProxy::TEvReadSessionStatus, Handle); // from read sessions info builder proxy
            HFunc(TEvPQProxy::TEvRead, Handle); // from gRPC
            HFunc(/* type alias */ TEvReadResponse, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvDirectReadResponse, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvDirectReadAck, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvDone, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvCloseSession, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvDieCommand, Handle);
            HFunc(TEvPQProxy::TEvPartitionReady, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReleased, Handle);  // from partitionActor
            HFunc(TEvPQProxy::TEvCommitCookie, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvCommitRange, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvStartRead, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvReleased, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvGetStatus, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvAuth, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvCommitDone, Handle); // from PartitionActor
            HFunc(TEvPQProxy::TEvPartitionStatus, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvUpdateSession, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvReadingStarted, Handle); // from partitionActor
            HFunc(TEvPQProxy::TEvReadingFinished, Handle); // from partitionActor


            // Balancer events
            HFunc(TEvPersQueue::TEvLockPartition, Handle); // can be sent to itself when reading without a consumer
            HFunc(TEvPersQueue::TEvReleasePartition, Handle);
            HFunc(TEvPersQueue::TEvError, Handle);

            // pipe events
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            // system events
            HFunc(TEvents::TEvWakeup, Handle);

        default:
            break;
        }
    }

    bool ReadFromStreamOrDie(const TActorContext& ctx);
    bool WriteToStreamOrDie(const TActorContext& ctx, TServerMessage&& response, bool finish = false);
    bool SendControlMessage(TPartitionId id, TServerMessage&& message, const TActorContext& ctx);

    // grpc events
    void Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvNotifiedWhenDone::TPtr& ev, const TActorContext &ctx);
    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);

    // proxy events
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx);
    void Handle(typename TEvReadInit::TPtr& ev,  const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev,  const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx);
    void Handle(typename TEvReadResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDirectReadResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDirectReadAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReleased::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvUpdateSession::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadingStarted::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadingFinished::TPtr& ev, const TActorContext& ctx);

    // Balancer events
    void Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx); // can be sent to itself when reading without a consumer
    void Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx);

    // pipe events
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    // system events
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    TActorId CreatePipeClient(ui64 tabletId, const TActorContext& ctx);
    void ProcessBalancerDead(ui64 tabletId, const TActorContext& ctx);

    void RunAuthActor(const TActorContext& ctx);
    void RecheckACL(const TActorContext& ctx);
    void InitSession(const TActorContext& ctx);
    void RegisterSession(const TString& topic, const TActorId& pipe, const TVector<ui32>& groups, const TActorContext& ctx);
    void CloseSession(PersQueue::ErrorCode::ErrorCode code, const TString& reason, const TActorContext& ctx);
    void SendLockPartitionToSelf(ui32 partitionId, TString topicName, TTopicHolder topic, const TActorContext& ctx);

    void SetupCounters();
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic);
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic,
        const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId);

    void ProcessReads(const TActorContext& ctx);
    ui64 PrepareResponse(typename TFormedReadResponse<TServerMessage>::TPtr formedResponse);
    void ProcessAnswer(typename TFormedReadResponse<TServerMessage>::TPtr formedResponse, const TActorContext& ctx);

    void DropPartition(TPartitionsMapIterator& it, const TActorContext& ctx);
    void ReleasePartition(TPartitionsMapIterator& it, bool couldBeReads, const TActorContext& ctx);
    void SendReleaseSignal(TPartitionActorInfo& partition, bool kill, const TActorContext& ctx);
    void InformBalancerAboutRelease(TPartitionsMapIterator it, const TActorContext& ctx);

    static ui32 NormalizeMaxReadMessagesCount(ui32 sourceValue);
    static ui32 NormalizeMaxReadSize(ui32 sourceValue);

private:
    std::unique_ptr</* type alias */ TEvStreamReadRequest> Request;
    const TString ClientDC;
    const TInstant StartTimestamp;

    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;
    TIntrusiveConstPtr<NACLib::TUserToken> Token;

    TString ClientId;
    TString ClientPath;
    TString Session;
    TString PeerName;

    bool CommitsDisabled;
    bool ReadWithoutConsumer;

    bool InitDone;
    bool RangesMode;

    ui32 MaxReadMessagesCount;
    ui32 MaxReadSize;
    i64 MaxTimeLagMs;
    i64 ReadTimestampMs;
    i64 ReadSizeBudget;

    TString Auth;

    bool ForceACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;

    THashSet<TActorId> ActualPartitionActors;
    THashMap<ui64, std::pair<ui32, ui64>> BalancerGeneration;
    ui64 NextAssignId;
    TPartitionsMap Partitions; // assignId -> info

    THashMap<TString, TTopicHolder> Topics; // topic -> info
    THashMap<TString, NPersQueue::TTopicConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashSet<TString> TopicsToResolve;
    THashMap<TString, TVector<ui32>> TopicGroups;
    THashMap<TString, i64> ReadFromTimestamp;
    THashMap<TString, i64> MaxLagByTopic;

    bool ReadOnlyLocal;
    TDuration CommitInterval;

    TSet<TPartitionInfo> AvailablePartitions;

    // Partition actor -> TFormedReadResponse answer that has this partition.
    // PartitionsTookPartInRead in formed read response contain this actor id.
    THashMap<TActorId, typename TFormedReadResponse<TServerMessage>::TPtr> PartitionToReadResponse;

    // Response that currenly pending quota
    typename TFormedReadResponse<TServerMessage>::TPtr PendingQuota;

    // Responses that will be quoted next
    std::deque<typename TFormedReadResponse<TServerMessage>::TPtr> WaitingQuota;

    struct TControlMessages {
        TVector<TServerMessage> ControlMessages;
        ui32 Infly = 0;
    };

    TMap<TPartitionId, TControlMessages> PartitionToControlMessages;

    std::deque<THolder<TEvPQProxy::TEvRead>> Reads;

    ui64 Cookie;

    struct TCommitInfo {
        ui64 StartReadId;
        ui32 Partitions;
    };

    TMap<ui64, TCommitInfo> Commits; // readid -> TCommitInfo

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    ::NMonitoring::TDynamicCounters::TCounterPtr SessionsCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionsActive;

    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr PipeReconnects;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflight;
    ui64 BytesInflight_;
    ui64 RequestedBytes;
    ui32 ReadsInfly;
    std::queue<ui64> ActiveWrites;

    NPQ::TPercentileCounter PartsPerSession;

    THashMap<TString, TTopicCounters> TopicCounters;
    THashMap<TString, ui32> NumPartitionsFromTopic;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    NPQ::TMultiCounter SLITotal;
    NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NPQ::TPercentileCounter InitLatency;
    NPQ::TPercentileCounter ReadLatency;
    NPQ::TPercentileCounter ReadLatencyFromDisk;
    NPQ::TPercentileCounter CommitLatency;
    NPQ::TMultiCounter SLIBigLatency;
    NPQ::TMultiCounter SLIBigReadLatency;
    NPQ::TMultiCounter ReadsTotal;

    NPersQueue::TTopicsListController TopicsHandler;
    NPersQueue::TTopicsToConverter TopicsList;

    bool DirectRead;
    bool AutoPartitioningSupport;
};

}
