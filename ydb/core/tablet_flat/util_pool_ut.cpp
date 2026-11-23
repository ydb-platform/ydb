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

    Y_UNIT_TEST(RemembersTheLargerRemainingTail) {
        TMemoryPool pool(128);
        auto* first = static_cast<char*>(pool.Allocate(16));
        pool.Allocate(128);
        auto* third = static_cast<char*>(pool.Allocate(256));
        const size_t total = pool.Total();

        // The first chunk has a larger tail than the more recent second chunk
        UNIT_ASSERT(pool.Allocate(64) == first + 16);
        UNIT_ASSERT_VALUES_EQUAL(pool.Total(), total);

        pool.Allocate(512);
        // The third chunk's tail replaces the now smaller cached tail
        UNIT_ASSERT(pool.Allocate(32) == third + 256);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), pool.Total());
    }

    Y_UNIT_TEST(RollbackRestoresAlignedCachedTail) {
        TMemoryPool pool(256);
        pool.Allocate(128);
        pool.Allocate(256);
        const size_t used = pool.Used();
        const size_t wasted = pool.Wasted();
        const size_t available = pool.Available();

        pool.BeginTransaction();
        void* aligned = pool.Allocate(16, 64);
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(aligned) % 64, 0);
        const size_t consumed = pool.Used() - used;
        UNIT_ASSERT_GE(consumed, 16);
        UNIT_ASSERT_LT(consumed, 16 + 64);
        UNIT_ASSERT_VALUES_EQUAL(pool.Wasted(), wasted - consumed);
        UNIT_ASSERT_VALUES_EQUAL(pool.Available(), available - consumed);
        pool.RollbackTransaction();

        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), used);
        UNIT_ASSERT_VALUES_EQUAL(pool.Wasted(), wasted);
        UNIT_ASSERT_VALUES_EQUAL(pool.Available(), available);
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocate(16, 64), aligned);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), pool.Total());
    }

    Y_UNIT_TEST(RollbackRestoresRepeatedlyReplacedCachedTail) {
        TMemoryPool pool(128);
        auto* first = static_cast<char*>(pool.Allocate(80));
        auto* second = static_cast<char*>(pool.Allocate(128));
        memset(first, 'a', 80);
        memset(second, 'b', 128);
        const size_t used = pool.Used();

        pool.BeginTransaction();
        void* cached = pool.Allocate(8);
        UNIT_ASSERT(cached == first + 80);
        auto* large = static_cast<char*>(pool.Allocate(512));
        UNIT_ASSERT(pool.Allocate(16) == second + 128);
        auto* larger = static_cast<char*>(pool.Allocate(1024));
        UNIT_ASSERT(pool.Allocate(32) == large + 512);
        void* largest = pool.Allocate(2048);
        UNIT_ASSERT(pool.Allocate(64) == larger + 1024);
        const size_t total = pool.Total();
        pool.RollbackTransaction();

        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), used);
        UNIT_ASSERT_VALUES_EQUAL(pool.Total(), total);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), total);
        UNIT_ASSERT_VALUES_EQUAL(first[79], 'a');
        UNIT_ASSERT_VALUES_EQUAL(second[127], 'b');

        // Replay both the cached allocation and a retained chunk after rollback
        pool.BeginTransaction();
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocate(8), cached);
        UNIT_ASSERT(pool.Allocate(512) == large);
        UNIT_ASSERT(pool.Allocate(16) == second + 128);
        UNIT_ASSERT(pool.Allocate(1024) == larger);
        UNIT_ASSERT(pool.Allocate(32) == large + 512);
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocate(2048), largest);
        UNIT_ASSERT(pool.Allocate(64) == larger + 1024);
        UNIT_ASSERT_VALUES_EQUAL(pool.Total(), total);
        pool.CommitTransaction();
        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), used + 8 + 512 + 16 + 1024 + 32 + 2048 + 64);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used() + pool.Wasted(), total);
    }

    Y_UNIT_TEST(RollbackRestoresTailCreatedDuringTransaction) {
        TMemoryPool pool(128);
        const size_t available = pool.Available();
        pool.BeginTransaction();
        void* large = pool.Allocate(128);
        void* small = pool.Allocate(16);
        const size_t total = pool.Total();
        pool.RollbackTransaction();

        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.Wasted(), total);
        UNIT_ASSERT_GT(pool.Available(), available);
        pool.BeginTransaction();
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocate(128), large);
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocate(16), small);
        pool.CommitTransaction();
        UNIT_ASSERT_VALUES_EQUAL(pool.Total(), total);
        UNIT_ASSERT_VALUES_EQUAL(pool.Used(), 128 + 16);
    }

} // Y_UNIT_TEST_SUITE(TMemoryPoolTest)

} // namespace NKikimr::NUtil
