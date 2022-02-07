#pragma once

#include "latch.h"

template <typename TThis>
class TComplexConditionSemaphore {
private:
    TLatch Latch;

public:
    void Updated() {
        if (GetThis()->TryWait()) {
            Latch.Unlock();
        }
    }

    void Wait() {
        while (!GetThis()->TryWait()) {
            Latch.Lock();
            if (GetThis()->TryWait()) {
                Latch.Unlock();
                return;
            }
            Latch.Wait();
        }
    }

    bool IsLocked() {
        return Latch.IsLocked();
    }

private:
    TThis* GetThis() {
        return static_cast<TThis*>(this);
    }
};
