#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

#include <functional>

namespace NActors {

    struct TActorLivenessCheckTarget {
        TActorId ActorId;
        ui64 Cookie = 0;
    };

    struct TActorLivenessCheckerCallbacks {
        // Callbacks run synchronously in the checker actor context.
        std::function<void(const TActorLivenessCheckTarget&)> OnActorDead;
        // Called exactly once after every target has produced a liveness response.
        std::function<void(const TVector<TActorLivenessCheckTarget>&)> OnComplete;
    };

    IActor* CreateActorLivenessChecker(
        TVector<TActorLivenessCheckTarget> targets,
        TActorLivenessCheckerCallbacks callbacks = {});

}
