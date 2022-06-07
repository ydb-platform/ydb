#include <library/cpp/testing/unittest/registar.h>

#include "cc_semaphore.h"

#include <library/cpp/deprecated/atomic/atomic.h>

namespace {
    struct TTestSemaphore: public TComplexConditionSemaphore<TTestSemaphore> {
        TAtomic Current;

        TTestSemaphore()
            : Current(0)
        {
        }

        bool TryWait() {
            return AtomicGet(Current) > 0;
        }

        void Aquire() {
            Wait();
            AtomicDecrement(Current);
        }

        void Release() {
            AtomicIncrement(Current);
            Updated();
        }
    };
}

Y_UNIT_TEST_SUITE(TComplexConditionSemaphore) {
    Y_UNIT_TEST(Simple) {
        TTestSemaphore sema;
        UNIT_ASSERT(!sema.TryWait());
        sema.Release();
        UNIT_ASSERT(sema.TryWait());
        sema.Release();
        UNIT_ASSERT(sema.TryWait());
        sema.Aquire();
        UNIT_ASSERT(sema.TryWait());
        sema.Aquire();
        UNIT_ASSERT(!sema.TryWait());
    }
}
