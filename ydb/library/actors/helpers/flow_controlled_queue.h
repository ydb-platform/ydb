#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NActors {

    struct TFlowControlledQueueConfig {
        ui32 MinAllowedInFly = 20;
        ui32 MaxAllowedInFly = 100;
        ui32 TargetDynamicRate = 0;

        TDuration MinTrackedLatency = TDuration::MilliSeconds(20);
        ui32 LatencyFactor = 4;
    };

    template <class TEnum = IActor::EActivityType>
    IActor* CreateFlowControlledRequestQueue(TActorId targetId, const TEnum activity = IActor::EActivityType::ACTORLIB_COMMON, const TFlowControlledQueueConfig &config = TFlowControlledQueueConfig());

}
