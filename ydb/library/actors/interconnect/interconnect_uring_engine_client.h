#pragma once

#include "interconnect_uring_engine.h"

namespace NActors {

    class TActorSystem;

    // Creates an actor-system client using the destruction settings from common.
    // SetClient calls Start, which registers engine shutdown before actor-system teardown.
    IUringEngineClientPtr CreateUringEngineClient(
        const TInterconnectProxyCommon& common,
        TActorSystem* actorSystem);

}
