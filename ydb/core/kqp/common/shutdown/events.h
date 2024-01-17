#pragma once
#include "state.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvInitiateSessionShutdown: public TEventLocal<TEvInitiateSessionShutdown, TKqpEvents::EvInitiateSessionShutdown> {
    ui32 SoftTimeoutMs;
    ui32 HardTimeoutMs;

    TEvInitiateSessionShutdown(ui32 softTimeoutMs, ui32 hardTimeoutMs)
        : SoftTimeoutMs(softTimeoutMs)
        , HardTimeoutMs(hardTimeoutMs) {
    }
};

struct TEvContinueShutdown: public TEventLocal<TEvContinueShutdown, TKqpEvents::EvContinueShutdown> {};

struct TEvInitiateShutdownRequest: public TEventLocal<TEvInitiateShutdownRequest, TKqpEvents::EvInitiateShutdownRequest> {
    TIntrusivePtr<TKqpShutdownState> ShutdownState;

    TEvInitiateShutdownRequest(TIntrusivePtr<TKqpShutdownState> ShutdownState)
        : ShutdownState(ShutdownState) {
    }
};

} // namespace NKikimr::NKqp
