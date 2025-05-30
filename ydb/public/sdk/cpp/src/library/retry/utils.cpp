#include "utils.h"

#include <util/random/random.h>

TDuration NRetryPrivate::AddRandomDelta(TDuration maxDelta) {
    if (maxDelta == TDuration::Zero()) {
        return TDuration::Zero();
    }

    const TDuration delta = TDuration::MicroSeconds(RandomNumber(2 * maxDelta.MicroSeconds()));
    return delta - maxDelta;
}

TDuration NRetryPrivate::AddIncrement(ui32 attempt, TDuration increment) {
    return TDuration::MicroSeconds(attempt * increment.MicroSeconds());
}

TDuration NRetryPrivate::AddExponentialMultiplier(ui32 attempt, TDuration exponentialMultiplier) {
    return TDuration::MicroSeconds((1ull << Min(63u, attempt)) * exponentialMultiplier.MicroSeconds());
}
