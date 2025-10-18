#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TMessageEnricherActor : public TBaseActor<TMessageEnricherActor>
                            , public TConstantLogPrefix {

    static constexpr TDuration Timeout = TDuration::Seconds(1);

public:
    TMessageEnricherActor(const ui32 partitionId, const TActorId& partitionActor, const TString& consumerName, std::deque<TReadResult>&& replies);

    void Bootstrap();
    void PassAway() override;
    TString BuildLogPrefix() const override;

private:
    void Handle(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    STFUNC(StateWork);

    void ProcessQueue();

private:
    const ui32 PartitionId;
    const TActorId PartitionActorId;
    const TString ConsumerName;
    std::deque<TReadResult> Queue;
    TBackoff Backoff;
    ui64 Cookie = 0;

    std::unique_ptr<TEvPersQueue::TEvMLPReadResponse> PendingResponse;
};

} // namespace NKikimr::NPQ::NMLP
