#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

class TBatch;
class TStorage;
class TDetailedMetrics;

using namespace NActors;

class TConsumerActor : public TBaseTabletActor<TConsumerActor>
                     , public TConstantLogPrefix {
    static constexpr TDuration WakeupInterval = TDuration::Seconds(1);
    static constexpr TDuration NoMessagesTimeout = TDuration::Seconds(1);

public:
    TConsumerActor(const TString& database, ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId,
        const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig& topicConfig, const NKikimrPQ::TPQTabletConfig::TConsumer& config,
        std::optional<TDuration> retentionPeriod, ui64 partitionEndOffset, NMonitoring::TDynamicCounterPtr& detailedMetricsRoot);

    void Bootstrap();
    void PassAway() override;

protected:
    TString BuildLogPrefix() const override;

private:
    void Queue(TEvPQ::TEvMLPReadRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPCommitRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPUnlockRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPPurgeRequest::TPtr&);

    void Handle(TEvPQ::TEvMLPReadRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPCommitRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPUnlockRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPPurgeRequest::TPtr&);

    void Handle(TEvPQ::TEvMLPConsumerUpdateConfig::TPtr&);
    void HandleInit(TEvPQ::TEvEndOffsetChanged::TPtr&);
    void Handle(TEvPQ::TEvEndOffsetChanged::TPtr&);
    void Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr&);

    void HandleOnInit(TEvKeyValue::TEvResponse::TPtr&);
    void Handle(TEvKeyValue::TEvResponse::TPtr&);

    void Handle(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);

    void HandleOnInit(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPersQueue::TEvResponse::TPtr&);

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    void HandleOnWork(TEvents::TEvWakeup::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    void Handle(TEvPQ::TEvMLPDLQMoverResponse::TPtr&);

    void Handle(TEvPQ::TEvMLPConsumerMonRequest::TPtr&);

    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateWrite);

    void Restart(TString&& error);

    void ProcessEventQueue();
    bool FetchMessagesIfNeeded();
    void ReadSnapshot();
    void Persist();
    void MoveToDLQIfPossible();

    void CommitIfNeeded();
    void UpdateStorageConfig();
    void InitializeDetailedMetrics();

    size_t RequiredToFetchMessageCount() const;
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

    void UpdateMetrics();

    bool UseForReading() const;
    void NotifyPQRB(bool force = false);

private:
    const TString Database;
    const ui32 PartitionId;
    const TActorId PartitionActorId;
    NKikimrPQ::TPQTabletConfig TopicConfig;
    NKikimrPQ::TPQTabletConfig::TConsumer Config;
    std::optional<TDuration> RetentionPeriod;
    ui64 PartitionEndOffset;

    bool FetchInProgress = false;
    ui64 FetchCookie = 0;
    ui64 LastCommittedOffset = 0;

    TActorId DLQMoverActorId;

    std::unique_ptr<TStorage> Storage;

    std::deque<TEvPQ::TEvMLPReadRequest::TPtr> ReadRequestsQueue;
    std::deque<TEvPQ::TEvMLPCommitRequest::TPtr> CommitRequestsQueue;
    std::deque<TEvPQ::TEvMLPUnlockRequest::TPtr> UnlockRequestsQueue;
    std::deque<TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr> ChangeMessageDeadlineRequestsQueue;
    std::deque<TEvPQ::TEvMLPPurgeRequest::TPtr> PurgeRequestsQueue;

    std::deque<TReadResult> PendingReadQueue;
    std::deque<TResult> PendingCommitQueue;
    std::deque<TResult> PendingUnlockQueue;
    std::deque<TResult> PendingChangeMessageDeadlineQueue;
    std::deque<TResult> PendingPurgeQueue;

    ui64 LastWALIndex = 0;
    bool HasSnapshot = false;

    bool FirstPipeCacheRequest = true;

    ui64 CPUUsageMetric = 0;
    NMonitoring::TDynamicCounterPtr DetailedMetricsRoot;
    std::unique_ptr<TDetailedMetrics> DetailedMetrics;

    TInstant LastTimeWithMessages;
    bool LastUseForReading = false;
};

class TDetailedMetrics {
public:
    TDetailedMetrics(const NKikimrPQ::TPQTabletConfig::TConsumer& consumerConfig, ::NMonitoring::TDynamicCounterPtr& root);
    ~TDetailedMetrics();

    void UpdateMetrics(const TMetrics& metrics);

private:
    NMonitoring::TDynamicCounters::TCounterPtr InflightCommittedCount;
    NMonitoring::TDynamicCounters::TCounterPtr InflightLockedCount;
    NMonitoring::TDynamicCounters::TCounterPtr InflightDelayedCount;
    NMonitoring::TDynamicCounters::TCounterPtr InflightUnlockedCount;
    NMonitoring::TDynamicCounters::TCounterPtr InflightScheduledToDLQCount;
    NMonitoring::TDynamicCounters::TCounterPtr CommittedCount;
    NMonitoring::TDynamicCounters::TCounterPtr PurgedCount;

    NMonitoring::THistogramPtr MessageLocks;
    NMonitoring::THistogramPtr MessageLockingDuration;
    NMonitoring::THistogramPtr WaitingLockingDuration;

    NMonitoring::TDynamicCounters::TCounterPtr DeletedByRetentionPolicy;
    NMonitoring::TDynamicCounters::TCounterPtr DeletedByDeadlinePolicy;
    NMonitoring::TDynamicCounters::TCounterPtr DeletedByMovedToDLQ;
};

}
