#include "util_pool.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <string.h>

namespace NKikimr::NUtil {

Y_UNIT_TEST_SUITE(TMemoryPoolTest) {

    Y_UNIT_TEST(AllocOneByte) {
        TMemoryPool pool(1);
        UNIT_ASSERT_C(pool.Used() == 0, pool.Used());
        UNIT_ASSERT_C(pool.Wasted() > 0, pool.Wasted());
        UNIT_ASSERT_C(pool.Available() >= 1, pool.Available());
        size_t initialWasted = pool.Wasted();
        size_t initialAvailable = pool.Available();
        void* ptr = pool.Allocate(1);
        Y_UNUSED(ptr);
        UNIT_ASSERT_C(pool.Used() >= 1, pool.Used());
        UNIT_ASSERT_C(pool.Used() + pool.Wasted() == initialWasted, pool.Used() << " + " << pool.Wasted());
        UNIT_ASSERT_C(pool.Used() + pool.Available() == initialAvailable, pool.Used() << " + " << pool.Available());
    }

    Y_UNIT_TEST(AppendString) {
        TMemoryPool pool(128);
        const char* str = "Hello, world!";
        char* ptr = (char*)pool.Append(str, ::strlen(str) + 1);
        UNIT_ASSERT(::strcmp(ptr, str) == 0);
    }

    void DoTransactions(size_t align = 0) {
        TMemoryPool pool(128);
        size_t expectedUsed = 0;
        size_t expectedSize = 0;
        for (int i = 0; i < 1000; ++i) {
            size_t initialUsed = pool.Used();
            pool.BeginTransaction();
            ui32 r = RandomNumber<ui32>(20);
            int count = (r >> 1) + 1;
            bool commit = r & 1;
            for (int i = 0; i < count; ++i) {
                if (align) {
                    void* ptr = pool.Allocate(1, align);
                    UNIT_ASSERT(ptr);
                } else {
                    void* ptr = pool.Allocate(1);
                    UNIT_ASSERT(ptr);
                }
            }
            expectedSize = Max(expectedSize, pool.Used() + pool.Wasted());
            if (commit) {
                expectedUsed += pool.Used() - initialUsed;
                pool.CommitTransaction();
            } else {
                pool.RollbackTransaction();
            }
        }
        UNIT_ASSERT_C(pool.Used() == expectedUsed, pool.Used() << " != " << expectedUsed);
        UNIT_ASSERT_C(pool.Used() + pool.Wasted() == expectedSize, pool.Used() << " + " << pool.Wasted() << " != " << expectedSize);
    }

    Y_UNIT_TEST(Transactions) {
        DoTransactions();
    }

    Y_UNIT_TEST(TransactionsWithAlignment) {
        DoTransactions(16);
    }

    Y_UNIT_TEST(LongRollback) {
        TMemoryPool pool(1);
        UNIT_ASSERT_C(pool.Used() == 0, pool.Used());
        UNIT_ASSERT_C(pool.Used() + pool.Wasted() == pool.Total(), pool.Used() << " + " << pool.Wasted() << " != " << pool.Total());
        for (int j = 0; j < 2; ++j) {
            pool.BeginTransaction();
            for (int i = 0; i < 1000; ++i) {
                void* ptr = pool.Allocate(3);
                UNIT_ASSERT(ptr);
            }
            UNIT_ASSERT_C(pool.Used() > 0, pool.Used());
            UNIT_ASSERT_C(pool.Wasted() < pool.Total(), pool.Wasted() << " >= " << pool.Total());
            UNIT_ASSERT_C(pool.Used() + pool.Wasted() == pool.Total(), pool.Used() << " + " << pool.Wasted() << " != " << pool.Total());
            pool.RollbackTransaction();
            UNIT_ASSERT_C(pool.Used() == 0, pool.Used());
            UNIT_ASSERT_C(pool.Used() + pool.Wasted() == pool.Total(), pool.Used() << " + " << pool.Wasted() << " != " << pool.Total());
        }
    }

} // Y_UNIT_TEST_SUITE(TMemoryPoolTest)

} // namespace NKikimr::NUtil
