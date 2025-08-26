#pragma once

#include <util/datetime/base.h>
#include <util/random/random.h>

namespace NKikimr::NReplication {

struct TBackoff {

    TBackoff(size_t maxRetries = 10, TDuration initialDelay = TDuration::Seconds(1), TDuration maxDelay = TDuration::Minutes(15))
        : MaxRetries(maxRetries)
        , InitialDelay(initialDelay)
        , MaxDelay(maxDelay)
        , Iteration(0) {
    }

    TDuration Next() {
        TDuration delay = TDuration::MilliSeconds(InitialDelay.MilliSeconds() << Iteration++);
        delay = std::min(MaxDelay, delay);
        delay += TDuration::MilliSeconds(RandomNumber<size_t>(delay.MilliSeconds() / 4));
        return delay;
    }

    bool HasMore() const {
        return Iteration < MaxRetries;
    }

    operator bool() const {
        return HasMore();
    }

    const size_t MaxRetries;
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    size_t Iteration;
};
}
