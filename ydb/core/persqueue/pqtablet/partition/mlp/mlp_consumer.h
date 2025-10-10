#pragma once

#include "mlp.h"
#include "mlp_storage.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NPQ::NMLP {

using namespace NActors;

class TConsumerActor : public NActors::TActorBootstrapped<TConsumerActor> {

public:
    TConsumerActor(const TActorId& tabletActorId, const TActorId& partitionActorId);

protected:
    void Bootstrap();

private:
    void Handle();

    STFUNC(StateInit);

    STFUNC(StateWork);

private:
    const TActorId TabletActorId;
    const TActorId PartitionActorId;
};

}
