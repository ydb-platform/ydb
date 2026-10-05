#include "simple_leaky_bucket.h"

#include <algorithm>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TDuration MinWaitTime = TDuration::MicroSeconds(5);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSimpleLeakyBucket::TSimpleLeakyBucket(
    TInstant ts,
    double rate,
    double initialBudget)
    : Rate(rate)
    , MaxBudget(initialBudget)
    , MinDelayedBudget(-Rate * MinWaitTime.SecondsFloat())
    , Budget(initialBudget)
    , LastUpdateTs(ts)
{
    Y_ABORT_UNLESS(Rate > 0);
    Y_ABORT_UNLESS(MinDelayedBudget < 0);
}

TDuration TSimpleLeakyBucket::Register(TInstant ts, double valueToSpend)
{
    Y_DEBUG_ABORT_UNLESS(valueToSpend > 0);

    const TDuration timePassed = ts - LastUpdateTs;
    if (timePassed) {
        Budget = Min(Budget + Rate * timePassed.SecondsFloat(), MaxBudget);
    }

    LastUpdateTs = ts;
    Budget -= valueToSpend;

    return Budget >= MinDelayedBudget ? TDuration()
                                      : TDuration::Seconds(-Budget / Rate);
}

double TSimpleLeakyBucket::GetBudget() const
{
    return Budget;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
