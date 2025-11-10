#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TDLQMoverActor : public TBaseActor<TDLQMoverActor>
                     , public TConstantLogPrefix {

    static constexpr TDuration Timeout = TDuration::Seconds(5);

public:
    TDLQMoverActor(const TString& database,
                   const ui64 tabletId,
                   const ui32 partitionId,
                   const TString& consumerName,
                   const TString& destinationTopic,
                   std::deque<ui64>&& offsets);

    void Bootstrap();
    void PassAway() override;

private:
    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    STFUNC(StateWork);

    void ProcessQueue();

private:
    const TString Database;
    const ui64 TabletId;
    const ui32 PartitionId;
    const TString ConsumerName;
    const ui64 ConsumerGeneration;
    const TString DestinationTopic;
    ui64 FirstMessageSeqNo;
    std::deque<ui64> Queue;
};

} // namespace NKikimr::NPQ::NMLP
