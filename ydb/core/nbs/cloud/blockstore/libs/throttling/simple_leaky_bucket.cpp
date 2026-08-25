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
    , BonusBudget(Rate * MinWaitTime.SecondsFloat())
    , Budget(initialBudget)
    , LastUpdateTs(ts)
{}

TDuration TSimpleLeakyBucket::Register(TInstant ts, double valueToSpent)
{
    Y_DEBUG_ABORT_UNLESS(valueToSpent > 0);

    const TDuration timePassed = ts - LastUpdateTs;
    if (timePassed) {
        Budget = std::min(Budget + Rate * timePassed.SecondsFloat(), MaxBudget);
    }
    LastUpdateTs = ts;

    if (Budget + BonusBudget >= valueToSpent) {
        Budget -= valueToSpent;
        return {};
    }

    Budget -= valueToSpent;
    return TDuration::Seconds(-Budget / Rate);
}

double TSimpleLeakyBucket::GetBudget() const
{
    return Budget;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
