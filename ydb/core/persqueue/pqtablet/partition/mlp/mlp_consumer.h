#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

class TBatch;
class TStorage;

using namespace NActors;

class TConsumerActor : public TBaseTabletActor<TConsumerActor>
                     , public TConstantLogPrefix {
    friend class TSerializer;

    static constexpr TDuration WakeupInterval = TDuration::Seconds(1);

public:
    TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig::TConsumer& config);

    void Bootstrap();
    void PassAway() override;

protected:
    TString BuildLogPrefix() const override;

private:
    void Queue(TEvPersQueue::TEvMLPReadRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPCommitRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPUnlockRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    void Handle(TEvPersQueue::TEvMLPReadRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPUnlockRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    void HandleOnInit(TEvKeyValue::TEvResponse::TPtr&);
    void HandleOnWrite(TEvKeyValue::TEvResponse::TPtr&);

    void HandleOnInit(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);

    void HandleOnWork(TEvents::TEvWakeup::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateWrite);

    void Restart(TString&& error);

    void ProcessEventQueue();
    bool FetchMessagesIfNeeded();
    void ReadSnapshot();
    void PersistSnapshot();

    void Commit();
    
private:
    const ui32 PartitionId;
    const TActorId PartitionActorId;
    const NKikimrPQ::TPQTabletConfig::TConsumer Config;

    bool FetchInProgress = false;
    ui64 FetchCookie = 0;
    ui64 LastCommittedOffset = 0;

    std::unique_ptr<TStorage> Storage;

    std::deque<TEvPersQueue::TEvMLPReadRequest::TPtr> ReadRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPCommitRequest::TPtr> CommitRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPUnlockRequest::TPtr> UnlockRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr> ChangeMessageDeadlineRequestsQueue;

    std::deque<TReadResult> PendingReadQueue;
    std::deque<TResult> PendingCommitQueue;
    std::deque<TResult> PendingUnlockQueue;
    std::deque<TResult> PendingChangeMessageDeadlineQueue;
};

}
