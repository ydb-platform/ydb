#pragma once

#include <util/generic/ptr.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NFq {

struct IYqSharedResources : public TThrRefBase {
    using TPtr = TIntrusivePtr<IYqSharedResources>;

    virtual void Init(NActors::TActorSystem* actorSystem) = 0;

    // Called after actor system stop.
    virtual void Stop() = 0;
};

enum class EDbPoolId {
    MAIN = 0
};

} // NFq
