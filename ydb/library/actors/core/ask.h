#pragma once

#include "actor.h"
#include "event.h"

#include <library/cpp/threading/future/future.h>

namespace NActors {
    /**
     * See `TActorSystem::Ask`.
     */
    std::unique_ptr<IActor> MakeAskActor(
        TMaybe<ui32> expectedEventType,
        TActorId recipient,
        std::unique_ptr<IEventBase> event,
        TDuration timeout,
        const NThreading::TPromise<std::unique_ptr<IEventBase>>& promise);
}
