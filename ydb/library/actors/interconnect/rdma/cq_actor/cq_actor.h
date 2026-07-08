#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NActors {
class IActor;
}

namespace NMonitoring {
    struct TDynamicCounters;
}

namespace NInterconnect::NRdma {

enum class ECqMode : ui8 {
    POLLING = 0,
    EVENT = 1
};

struct TRdmaRuntimeParams;

/*
 * Creates CQ actor - abstraction to commuticate with CQ from actor system.
 * creates at least one CQ per rdma context
 * maxCqe - max capacity of single queue under CQ actor abstruction. -1 - use limit from rdma context
 */
NActors::IActor* CreateCqActor(const TRdmaRuntimeParams& runtimeParams, ECqMode mode, NMonitoring::TDynamicCounters* counters);
NActors::TActorId MakeCqActorId();

}
