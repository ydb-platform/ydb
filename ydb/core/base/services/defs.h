#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/base/services/defs.h
#include <library/cpp/actors/core/defs.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/actorid.h>

namespace NKikimr {
    // actorlib is organic part of kikimr so we emulate global import by this directive
    using namespace NActors;
} // namespace NKikimr
