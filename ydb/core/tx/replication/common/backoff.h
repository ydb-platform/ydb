#pragma once

#include <util/datetime/base.h>
#include <util/random/random.h>

namespace NKikimr::NReplication {

struct TBackoff {

    static constexpr TDuration MinDelay = TDuration::MilliSeconds(5);

    TBackoff(size_t maxRetries = 10, TDuration initialDelay = TDuration::Seconds(1), TDuration maxDelay = TDuration::Minutes(15))
        : MaxRetries(maxRetries)
        , InitialDelay(std::max(initialDelay, MinDelay))
        , MaxDelay(maxDelay)
        , Iteration(0)
    {
    }

    TDuration Next() {
        TDuration delay = TDuration::MilliSeconds(InitialDelay.MilliSeconds() << Iteration++);
        delay = delay == TDuration::Zero() ? MaxDelay : std::min(MaxDelay, delay);
        delay += TDuration::MilliSeconds(RandomNumber<size_t>(delay.MilliSeconds() / 4));
        return delay;
    }

    bool HasMore() const {
        return Iteration < MaxRetries;
    }

    explicit operator bool() const {
        return HasMore();
    }

    const size_t MaxRetries;
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    size_t Iteration;
};

}
