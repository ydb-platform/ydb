#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

namespace NActors {

    struct TActorLivenessCheckTarget {
        TActorId ActorId;
        ui64 Cookie = 0;
    };

    // Forwards TEvActorDead to notify for each target confirmed dead, preserving
    // the target as event sender and Cookie as event cookie. Sends TEvGone after
    // every target has produced a liveness response.
    IActor* CreateActorLivenessChecker(
        TVector<TActorLivenessCheckTarget> targets,
        const TActorId& notify);

}
