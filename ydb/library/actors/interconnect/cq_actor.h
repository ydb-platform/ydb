#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NActors {
class IActor;
}

namespace NInterconnect::NRdma {

/*
 * Creates CQ actor - abstraction to commuticate with CQ from actor system.
 * creates at least one CQ per rdma context
 * maxCqe - max capacity of single queue under CQ actor abstruction. -1 - use limit from rdma context
 */
NActors::IActor* CreateCqActor(int maxCqe);
NActors::IActor* CreateCqMockActor(int maxCqe);
NActors::TActorId MakeCqActorId();

}
