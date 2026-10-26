#pragma once

#include "ydb_clients.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NBridgeSkipper {

struct TEvSkipper {
    enum EEvents {
        EvTick = EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 0x7200,
        EvTickComplete,
        EvStateReply,
        EvSelfCheckReply,
        EvUpdateReply,
        EvStop,
        EvStopped,
        EvEnd,
    };

    struct TEvTick : NActors::TEventLocal<TEvTick, EvTick> {};
    struct TEvTickComplete : NActors::TEventLocal<TEvTickComplete, EvTickComplete> {};
    struct TEvStop : NActors::TEventLocal<TEvStop, EvStop> {};
    struct TEvStopped : NActors::TEventLocal<TEvStopped, EvStopped> {};

    struct TEvStateReply : NActors::TEventLocal<TEvStateReply, EvStateReply> {
        TStateResult Result;

        explicit TEvStateReply(TStateResult result)
            : Result(std::move(result)) {
        }
    };

    struct TEvSelfCheckReply : NActors::TEventLocal<TEvSelfCheckReply, EvSelfCheckReply> {
        TSelfCheckResult Result;

        explicit TEvSelfCheckReply(TSelfCheckResult result)
            : Result(std::move(result)) {
        }
    };

    struct TEvUpdateReply : NActors::TEventLocal<TEvUpdateReply, EvUpdateReply> {
        TUpdateResult Result;

        explicit TEvUpdateReply(TUpdateResult result)
            : Result(std::move(result)) {
        }
    };
};

static_assert(TEvSkipper::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

} // namespace NBridgeSkipper
