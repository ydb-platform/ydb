#pragma once

#include "defs.h"

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr::NBsQueue {

// Special timer for debug purposes, which works with virtual time of TTestActorSystem
struct TActivationContextTimer {
    TActivationContextTimer()
        : CreationTimestamp(NActors::TActivationContext::Monotonic())
    {}

    double Passed() const {
        return (NActors::TActivationContext::Monotonic() - CreationTimestamp).SecondsFloat();
    }

    TMonotonic CreationTimestamp;
};

struct TBSQueueTimer {
    TBSQueueTimer(bool useActorSystemTime)
    {
        if (useActorSystemTime) {
            Timer.emplace<TActivationContextTimer>();
        } else {
            Timer.emplace<THPTimer>();
        }
    }

    std::variant<THPTimer, TActivationContextTimer> Timer;

    double Passed() const {
        return std::visit([](const auto& timer) -> double {
            return timer.Passed();
        }, Timer);
    }
};

} // namespace NKikimr::NBsQueue