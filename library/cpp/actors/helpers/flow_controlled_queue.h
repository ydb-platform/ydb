#pragma once 
 
#include <library/cpp/actors/core/actor.h>
 
namespace NActors { 
 
    struct TFlowControlledQueueConfig { 
        ui32 MinAllowedInFly = 20; 
        ui32 MaxAllowedInFly = 100; 
        ui32 TargetDynamicRate = 0; 
 
        TDuration MinTrackedLatency = TDuration::MilliSeconds(20); 
        ui32 LatencyFactor = 4; 
    }; 
 
    IActor* CreateFlowControlledRequestQueue(TActorId targetId, ui32 activity = IActor::ACTORLIB_COMMON, const TFlowControlledQueueConfig &config = TFlowControlledQueueConfig());
 
} 
