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

using namespace NActors;

class TConsumerActor : public TBaseTabletActor<TConsumerActor>
                     , public TConstantLogPrefix {
    static constexpr TDuration WakeupInterval = TDuration::Seconds(1);

public:
    TConsumerActor(const TString& database, ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId,
        const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig::TConsumer& config,
        std::optional<TDuration> retentionPeriod);

    void Bootstrap();
    void PassAway() override;

protected:
    TString BuildLogPrefix() const override;

private:
    void Queue(TEvPQ::TEvMLPReadRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPCommitRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPUnlockRequest::TPtr&);
    void Queue(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    void Handle(TEvPQ::TEvMLPReadRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPCommitRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPUnlockRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    void Handle(TEvPQ::TEvMLPConsumerUpdateConfig::TPtr&);
    void Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr&);

    void HandleOnInit(TEvKeyValue::TEvResponse::TPtr&);
    void Handle(TEvKeyValue::TEvResponse::TPtr&);

    void HandleOnInit(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    void HandleOnWork(TEvents::TEvWakeup::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    void Handle(TEvPQ::TEvMLPDLQMoverResponse::TPtr&);

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
    
    size_t RequiredToFetchMessageCount() const;
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

private:
    const TString Database;
    const ui32 PartitionId;
    const TActorId PartitionActorId;
    NKikimrPQ::TPQTabletConfig::TConsumer Config;
    std::optional<TDuration> RetentionPeriod;

    bool HasDataInProgress = false;
    bool FetchInProgress = false;
    ui64 FetchCookie = 0;
    ui64 LastCommittedOffset = 0;

    TConsumerMetrics Metrics;
    TActorId DLQMoverActorId;

    std::unique_ptr<TStorage> Storage;

    std::deque<TEvPQ::TEvMLPReadRequest::TPtr> ReadRequestsQueue;
    std::deque<TEvPQ::TEvMLPCommitRequest::TPtr> CommitRequestsQueue;
    std::deque<TEvPQ::TEvMLPUnlockRequest::TPtr> UnlockRequestsQueue;
    std::deque<TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr> ChangeMessageDeadlineRequestsQueue;

    std::deque<TReadResult> PendingReadQueue;
    std::deque<TResult> PendingCommitQueue;
    std::deque<TResult> PendingUnlockQueue;
    std::deque<TResult> PendingChangeMessageDeadlineQueue;

    ui64 LastWALIndex = 0;
    bool HasSnapshot = false;

    bool FirstPipeCacheRequest = true;
};

}
