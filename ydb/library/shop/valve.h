#pragma once

#include "resource.h"
#include <util/system/spinlock.h>

namespace NShop {

class TValve {
public:
    using TCost = i64;

private:
    TSpinLock Lock;

    TCost Consumed = 0;
    TCost Supply = 0;
    TCost Demand = 0;

public:
    bool CanConsume() const
    {
        auto guard = Guard(Lock);
        return Consumed <= Supply;
    }

    bool CanSupply() const
    {
        auto guard = Guard(Lock);
        return Supply < Demand;
    }

    void AddConsumed(TCost cost)
    {
        auto guard = Guard(Lock);
        Consumed += cost;
    }

    void AddSupply(TCost cost)
    {
        auto guard = Guard(Lock);
        Supply += cost;
    }

    void AddDemand(TCost cost)
    {
        auto guard = Guard(Lock);
        Demand += cost;
    }
};

}
