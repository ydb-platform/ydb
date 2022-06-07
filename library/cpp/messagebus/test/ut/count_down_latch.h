#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>

class TCountDownLatch {
private:
    TAtomic Current;
    TSystemEvent EventObject;

public:
    TCountDownLatch(unsigned initial)
        : Current(initial)
    {
    }

    void CountDown() {
        if (AtomicDecrement(Current) == 0) {
            EventObject.Signal();
        }
    }

    void Await() {
        EventObject.Wait();
    }

    bool Await(TDuration timeout) {
        return EventObject.WaitT(timeout);
    }
};
