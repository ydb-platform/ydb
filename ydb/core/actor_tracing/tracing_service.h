#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NActorTracing {

    NActors::IActor* CreateActorTracingService();

} // namespace NKikimr::NActorTracing
