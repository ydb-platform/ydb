#pragma once

#include <unordered_map>
#include "events.h"
#include "partition_id.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/services/persqueue_v1/actors/distributed_commit_helper.h>
#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/public/inflight_limiter.h>
#include <ydb/core/util/ulid.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>


namespace NKikimr::NGRpcProxy::V1 {

using namespace NActors;


struct TOffsetInfo {
    // find by read id
    bool operator<(ui64 readId) const {
        return ReadId < readId;
    }

    friend bool operator<(ui64 readId, const TOffsetInfo& info) {
        return readId < info.ReadId;
    }


    ui64 ReadId = 0;
    ui64 Offset = 0;
};

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


class TPartitionActor : public NActors::TActorBootstrapped<TPartitionActor>
                      , public NActors::IActorExceptionHandler {
private:
    static constexpr TDuration READ_TIMEOUT_DURATION = TDuration::Seconds(1);

    static constexpr TDuration WAIT_DATA = TDuration::Seconds(10);
    static constexpr TDuration PREWAIT_DATA = TDuration::Seconds(9);
    static constexpr TDuration READ_METRICS_UPDATE_INTERVAL = TDuration::Seconds(10);
    static constexpr TDuration WAIT_DELTA = TDuration::MilliSeconds(500);

    static constexpr ui32 MAX_PIPE_RESTARTS = 100; //after 100 restarts without progress kill session
    static constexpr ui32 RESTART_PIPE_DELAY_MS = 100;

    static constexpr ui32 MAX_COMMITS_INFLY = 1;


public:
     TPartitionActor(const TActorId& parentId, const TString& clientId, const TString& clientPath, const ui64 cookie,
                     const TString& session, const TPartitionId& partition, ui32 generation, ui32 step,
                     const ui64 tabletID, const TTopicCounters& counters, const bool commitsDisabled,
                     const TString& clientDC, bool rangesMode, const NPersQueue::TTopicConverterPtr& topic, const TString& database, bool directRead,
                     bool useMigrationProtocol, ui32 maxTimeLagMs, ui64 readTimestampMs, const TTopicHolder::TPtr& topicHolder,
                     const std::unordered_set<ui64>& notCommitedToFinishParents, ui64 partitionMaxInFlightBytes);
    ~TPartitionActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;
    bool OnUnhandledException(const std::exception& exc) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_PARTITION; }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvPQProxy::TEvDeadlineExceeded, Handle)

            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvPQProxy::TEvRead, Handle)
            HFunc(TEvPQProxy::TEvCommitCookie, Handle)
            HFunc(TEvPQProxy::TEvCommitRange, Handle)
            HFunc(TEvPQProxy::TEvLockPartition, Handle)
            HFunc(TEvPQProxy::TEvGetStatus, Handle)
            HFunc(TEvPQProxy::TEvRestartPipe, Handle)
            HFunc(TEvPQProxy::TEvDirectReadAck, Handle)
            HFunc(TEvPQProxy::TEvUpdateReadMetrics, Handle)

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);

            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);

            HFunc(TEvPQProxy::TEvParentCommitedToFinish, Handle);

        default:
            break;
        };
    }


    void Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvDirectReadAck::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvDeadlineExceeded::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvParentCommitedToFinish::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvUpdateReadMetrics::TPtr& ev, const TActorContext& ctx);

    void HandlePoison(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);
    void DoWakeup(const NActors::TActorContext& ctx);

    void InitLockPartition(const NActors::TActorContext& ctx);
    void InitStartReading(const NActors::TActorContext& ctx);
    void RestartDirectReadSession();
    void    OnDirectReadsRestored();
    [[nodiscard]] bool SendNextRestorePrepareOrForget();
    [[nodiscard]] bool SendNextRestorePublishRequest();
    void ResendRecentRequests();

    void RestartPipe(const NActors::TActorContext& ctx, const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode);
    void WaitDataInPartition(const NActors::TActorContext& ctx);
    void SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx);
    void MakeCommit(const TActorContext& ctx);
    void SendPublishDirectRead(const ui64 directReadId, const TActorContext& ctx);
    void SendForgetDirectRead(const ui64 directReadId, const TActorContext& ctx);
    void SendPartitionReady(const TActorContext& ctx);
    void CommitDone(ui64 cookie, const TActorContext& ctx);
    NKikimrClient::TPersQueueRequest MakeCreateSessionRequest(bool initial, ui64 cookie) const;
    NKikimrClient::TPersQueueRequest MakeReadRequest(ui64 readOffset, ui64 lastOffset, ui64 maxCount,
                                                                      ui64 maxSize, ui64 maxTimeLagMs, ui64 readTimestampMs,
                                                                      ui64 directReadId, ui64 sizeEstimate = 0) const;

    const std::set<NPQ::TPartitionGraph::Node*>& GetParents(std::shared_ptr<const NPQ::TPartitionGraph> partitionGraph) const;

    void HandleInit(const NKikimrClient::TPersQueuePartitionResponse& response, const TActorContext& ctx);
    void HandleDirectReadRestoreSession(const NKikimrClient::TPersQueuePartitionResponse& response, const TActorContext& ctx);
    void Handle(const NKikimrClient::TPersQueuePartitionResponse::TCmdPrepareDirectReadResult& response, const TActorContext& ctx);
    void Handle(const NKikimrClient::TPersQueuePartitionResponse::TCmdPublishDirectReadResult& response, const TActorContext& ctx);
    void Handle(const NKikimrClient::TCmdReadResult& response, const TActorContext& ctx);

private:
    const TActorId ParentId;
    const TString ClientId;
    const TString ClientPath;
    const ui64 Cookie;
    const TString Session;
    const TString ClientDC;

    const TPartitionId Partition;
    const ui32 Generation;
    const ui32 Step;

    const ui64 TabletID;

    const ui32 MaxTimeLagMs;
    const ui64 ReadTimestampMs;

    ui64 ReadOffset;
    ui64 ClientReadOffset;
    TMaybe<ui64> ClientCommitOffset;
    bool ClientHasAnyCommits;
    bool ClientVerifyReadOffset;
    ui64 CommittedOffset;
    ui64 WriteTimestampEstimateMs;

    ui64 ReadIdToResponse;
    ui64 ReadIdCommitted;
    TSet<ui64> NextCommits;
    TDisjointIntervalTree<ui64> NextRanges;

    bool RangesMode;
    std::deque<TOffsetInfo> Offsets;

    ui64 WTime;
    bool InitDone;
    bool StartReading;
    bool AllPrepareInited;
    bool FirstInit;
    TActorId PipeClient;
    ui32 PipeGeneration;
    ui64 TabletGeneration;
    ui64 NodeId;

    bool RequestInfly;
    NKikimrClient::TPersQueueRequest CurrentRequest;

    ui64 EndOffset;
    ui64 SizeLag;

    TString ReadGuid; // empty if not reading

    ui64 InitCookie = 1;

    std::set<ui64> WaitDataInfly;
    ui64 WaitDataCookie;
    bool WaitForData;

    bool LockCounted;
    struct TCommitInfo {
        ui64 StartReadId;
        ui64 Offset;
        TInstant StartTime;
    };

    std::deque<std::pair<ui64, TCommitInfo>> CommitsInfly; //ReadId, Offset
    std::unordered_map<ui64, std::shared_ptr<TDistributedCommitHelper>> Kqps;

    const TTopicHolder::TPtr TopicHolder;

    TTopicCounters Counters;

    bool CommitsDisabled;
    ui64 CommitCookie;
    NPersQueue::TTopicConverterPtr Topic;
    TString Database;

    bool DirectRead = false;

    ui64 DirectReadId = 1;
    std::map<ui64, NKikimrClient::TPersQueuePartitionResponse::TCmdPrepareDirectReadResult> DirectReadResults;
    std::set<ui64> PublishedDirectReads;

    std::map<ui64, NKikimrClient::TPersQueuePartitionResponse::TCmdPrepareDirectReadResult> DirectReadsToRestore;
    std::set<ui64> DirectReadsToPublish;
    std::set<ui64> UnpublishedDirectReads;
    std::set<ui64> DirectReadsToForget;

    NPQ::TInFlightController PartitionInFlightMemoryController;

    enum class EDirectReadRestoreStage {
        None,
        Session,
        Prepare,
        Publish,
        Forget
    };
    ui64 RestoredDirectReadId = 0;
    EDirectReadRestoreStage DirectReadRestoreStage = EDirectReadRestoreStage::None;

    bool UseMigrationProtocol;

    bool FirstRead;
    bool ReadingFinishedSent;

    std::unordered_set<ui64> NotCommitedToFinishParents;
};


}
