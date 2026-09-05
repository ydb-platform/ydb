#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {
NActors::TActorId QuerySessionPoolServiceActorId();
NActors::IActor* CreateQuerySessionPoolActor();
}
