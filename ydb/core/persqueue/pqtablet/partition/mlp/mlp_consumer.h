#pragma once

#include "mlp.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

using namespace NActors;

class TConsumerActor : public TBaseActor<TConsumerActor> {

public:
    TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig::TConsumer& config);

protected:
    void Bootstrap();
    void PassAway();
    const TString& GetLogPrefix() const;

private:
    void Queue(TEvPersQueue::TEvMLPReadRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPCommitRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPReleaseRequest::TPtr&);
    void Queue(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    void Handle(TEvPersQueue::TEvMLPReadRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPReleaseRequest::TPtr&);
    void Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr&);

    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateWrite);

    void ProcessEventQueue();

private:
    const TActorId TabletActorId;
    const TActorId PartitionActorId;
    const NKikimrPQ::TPQTabletConfig::TConsumer Config;

    std::unique_ptr<TStorage> Storage;

    std::deque<TEvPersQueue::TEvMLPReadRequest::TPtr> ReadRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPCommitRequest::TPtr> CommitRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPReleaseRequest::TPtr> ReleaseRequestsQueue;
    std::deque<TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr> ChangeMessageDeadlineRequestsQueue;
};

}
