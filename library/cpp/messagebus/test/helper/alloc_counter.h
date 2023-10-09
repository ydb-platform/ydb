#pragma once

#include <util/generic/noncopyable.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/yassert.h>

class TAllocCounter : TNonCopyable {
private:
    TAtomic* CountPtr;

public:
    TAllocCounter(TAtomic* countPtr)
        : CountPtr(countPtr)
    {
        AtomicIncrement(*CountPtr);
    }

    ~TAllocCounter() {
        Y_ABORT_UNLESS(AtomicDecrement(*CountPtr) >= 0, "released too many");
    }
};
