#pragma once

#include "ydb_clients.h"
#include "commands.h"

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
        EvInventoryReply,
        EvStop,
        EvStopped,
        EvCommand,
        EvCommandResult,
        EvReady,
        EvReadyAck,
        EvEnd,
    };

    struct TEvCommand : NActors::TEventLocal<TEvCommand, EvCommand> {
        TControllerCommand Command;

        explicit TEvCommand(TControllerCommand command)
            : Command(std::move(command)) {
        }
    };

    struct TEvCommandResult : NActors::TEventLocal<TEvCommandResult, EvCommandResult> {
        TCommandResult Result;

        explicit TEvCommandResult(TCommandResult result)
            : Result(std::move(result)) {
        }
    };

    // Private readiness protocol, never accepted over HTTP.
    struct TEvReady : NActors::TEventLocal<TEvReady, EvReady> {};
    struct TEvReadyAck : NActors::TEventLocal<TEvReadyAck, EvReadyAck> {};

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

    struct TEvInventoryReply : NActors::TEventLocal<TEvInventoryReply, EvInventoryReply> {
        TInventoryResult Result;

        explicit TEvInventoryReply(TInventoryResult result)
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
