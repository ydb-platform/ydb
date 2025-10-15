#pragma once

#include "mlp.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/actor.h>

namespace NKikimr::NPQ::NMLP {

using namespace NActors;

class TConsumerActor : public TBaseActor<TConsumerActor> {

public:
    TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, const TActorId& partitionActorId);

protected:
    void Bootstrap();
    const TString& GetLogPrefix() const;

private:
    void Handle();

    STFUNC(StateInit);

    STFUNC(StateWork);

private:
    const TActorId TabletActorId;
    const TActorId PartitionActorId;
};

}
