#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NInterconnect {
    // load responder -- lives on every node as a service actor
    NActors::IActor* CreateLoadResponderActor();
    NActors::TActorId MakeLoadResponderActorId(ui32 node);

    // load actor -- generates load with specific parameters
    struct TLoadParams {
        TString Name;
        ui32 Channel;
        TVector<ui32> NodeHops;             // node ids for the message route
        ui32 SizeMin, SizeMax;              // min and max size for payloads
        ui32 InFlyMax;                      // maximum number of in fly messages
        TDuration IntervalMin, IntervalMax; // min and max intervals between sending messages
        bool SoftLoad;                      // is the load soft?
        TDuration Duration;                 // test duration
        bool UseProtobufWithPayload;        // store payload separately
    };
    NActors::IActor* CreateLoadActor(const TLoadParams& params);

}
