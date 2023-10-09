#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/ptr.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NSysView {

class TScanLimiter : public TThrRefBase {
public:
    explicit TScanLimiter(i64 limit)
        : Limit(limit)
    {}

    bool Inc() {
        i64 newValue;
        i64 prevValue;
        do {
            prevValue = AtomicGet(Count);
            Y_ABORT_UNLESS(prevValue >= 0);
            if (Limit && prevValue >= Limit) {
                return false;
            }
            newValue = prevValue + 1;
        } while (!AtomicCas(&Count, newValue, prevValue));
        return true;
    }

    void Dec() {
        i64 newValue = AtomicDecrement(Count);
        Y_ABORT_UNLESS(newValue >= 0);
    }

private:
    const i64 Limit;
    TAtomic Count = 0;
};

} // NSysView
} // NKikimr
