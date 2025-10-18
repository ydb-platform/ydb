#pragma once

#include "mlp.h"
#include "mlp_types.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ::NMLP {

class TMessageEnricherActor : public TBaseActor<TMessageEnricherActor>
                            , public TConstantLogPrefix {

    static constexpr TDuration Timeout = TDuration::Seconds(1);

public:
    TMessageEnricherActor(const TActorId& partitionActor, std::deque<TReadResult>&& replies);

    void Bootstrap();
    void PassAway() override;

private:
    void Handle(TEvPQ::TEvProxyResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    STFUNC(StateWork);

private:
    const TActorId PartitionActorId;
    std::deque<TReadResult> Replies;
};

} // namespace NKikimr::NPQ::NMLP
