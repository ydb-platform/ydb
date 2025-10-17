#pragma once

#include "mlp.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

class TBatch;
class TStorage;

using namespace NActors;

class TConsumerActor : public TBaseActor<TConsumerActor>
                     , public TConstantLogPrefix {
    friend class TSerializer;

public:
    TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig::TConsumer& config);

protected:
    void Bootstrap();
    void PassAway() override;
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

    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateWrite);

    void Restart(TString&& error);

    void ProcessEventQueue();

    void ReadSnapshot();
    void PersistSnapshot();

private:
    const TActorId TabletActorId;
    const ui32 PartitionId;
    const TActorId PartitionActorId;
    const NKikimrPQ::TPQTabletConfig::TConsumer Config;

    std::unique_ptr<TStorage> Storage;
    std::unique_ptr<TBatch> Batch;

    std::deque<TEvPersQueue::TEvMLPReadRequest::TPtr> ReadRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPCommitRequest::TPtr> CommitRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPUnlockRequest::TPtr> UnlockRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr> ChangeMessageDeadlineRequestsQueue;

};

}
