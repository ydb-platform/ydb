#include <library/cpp/testing/unittest/registar.h>

#include "latch.h"

Y_UNIT_TEST_SUITE(TLatch) {
    Y_UNIT_TEST(Simple) {
        TLatch latch;
        UNIT_ASSERT(latch.TryWait());
        latch.Lock();
        UNIT_ASSERT(!latch.TryWait());
        latch.Lock();
        latch.Lock();
        UNIT_ASSERT(!latch.TryWait());
        latch.Unlock();
        UNIT_ASSERT(latch.TryWait());
        latch.Unlock();
        latch.Unlock();
        UNIT_ASSERT(latch.TryWait());
    }
}
