#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NActors {
class IActor;
}

namespace NMonitoring {
    struct TDynamicCounters;
}

namespace NInterconnect::NRdma {

/*
 * Creates CQ actor - abstraction to commuticate with CQ from actor system.
 * creates at least one CQ per rdma context
 * maxCqe - max capacity of single queue under CQ actor abstruction. -1 - use limit from rdma context
 */
NActors::IActor* CreateCqActor(int maxCqe, int maxWr, NMonitoring::TDynamicCounters* counters = nullptr);
NActors::TActorId MakeCqActorId();

}
