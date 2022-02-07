#pragma once

#include "actor.h"
#include "event.h"

#include <library/cpp/threading/future/future.h>

namespace NActors {
    /**
     * See `TActorSystem::Ask`.
     */
    THolder<IActor> MakeAskActor(
        TMaybe<ui32> expectedEventType,
        TActorId recipient,
        THolder<IEventBase> event,
        TDuration timeout,
        const NThreading::TPromise<THolder<IEventBase>>& promise);
}
