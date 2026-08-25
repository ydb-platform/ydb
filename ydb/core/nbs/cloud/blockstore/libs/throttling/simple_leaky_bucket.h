#pragma once

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// A budget-based rate limiter with a bounded positive balance.
//
// The budget is replenished at a fixed rate and capped at initialBudget, which
// is also the starting balance. Register immediately reserves the operation's
// cost. If this creates a debt, it returns the time needed to repay that debt
// before the operation can be executed. Timestamps passed to Register must be
// nondecreasing and the refill rate must be positive.
class TSimpleLeakyBucket
{
public:
    TSimpleLeakyBucket(TInstant ts, double rate, double initialBudget);

    // Returns the time to wait before executing the registered operation.
    [[nodiscard]] TDuration Register(TInstant ts, double valueToSpent);

    // Returns the budget at the last update. The value may be negative.
    [[nodiscard]] double GetBudget() const;

private:
    const double Rate;        // budget refill rate per second
    const double MaxBudget;   // maximum accumulated budget

    // Extra budget used to suppress very short waits. The operation may execute
    // immediately, but the corresponding debt is retained in the budget.
    const double BonusBudget;

    double Budget = 0;   // accumulated budget
    TInstant LastUpdateTs;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
