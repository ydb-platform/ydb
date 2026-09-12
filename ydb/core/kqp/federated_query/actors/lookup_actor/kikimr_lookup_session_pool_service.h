#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq::NDqSourceLookup {
NActors::TActorId QuerySessionPoolServiceActorId();
NActors::IActor* CreateQuerySessionPoolActor();
}
