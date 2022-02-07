#include <library/cpp/testing/unittest/registar.h>

#include "memory.h"

Y_UNIT_TEST_SUITE(MallocAligned) {
    Y_UNIT_TEST(Test) {
        for (size_t size = 0; size < 1000; ++size) {
            void* ptr = MallocAligned(size, 128);
            UNIT_ASSERT(uintptr_t(ptr) % 128 == 0);
            FreeAligned(ptr);
        }
    }
}
