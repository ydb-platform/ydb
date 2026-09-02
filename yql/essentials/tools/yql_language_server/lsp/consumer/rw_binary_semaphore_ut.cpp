#include "rw_binary_semaphore.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>
#include <util/system/rwlock.h>
#include <util/thread/pool.h>

using namespace NLsp;

Y_UNIT_TEST_SUITE(RWBinarySemaphoreTests) {

Y_UNIT_TEST(ReaderOnly) {
    TRWBinarySemaphore s;

    s.AcquireRead();
    s.ReleaseRead();

    s.AcquireRead();
    s.ReleaseRead();
}

Y_UNIT_TEST(WriterOnly) {
    TRWBinarySemaphore s;

    s.AcquireWrite();
    s.ReleaseWrite();

    s.AcquireWrite();
    s.ReleaseWrite();
}

Y_UNIT_TEST(ReaderReentrance) {
    TRWBinarySemaphore s;

    s.AcquireRead();
    s.AcquireRead();
    s.ReleaseRead();
    s.ReleaseRead();
}

Y_UNIT_TEST(RWInterference) {
    TRWBinarySemaphore s;

    s.AcquireRead();
    s.ReleaseRead();

    s.AcquireWrite();
    s.ReleaseWrite();

    s.AcquireRead();
    s.ReleaseRead();

    s.AcquireWrite();
    s.ReleaseWrite();

    s.AcquireRead();
    s.ReleaseRead();
}

Y_UNIT_TEST(Stress) {
    constexpr size_t Concurrency = 64;
    static_assert(1 <= Concurrency);

    constexpr size_t Iterations = 8 * 1024;

    TRWBinarySemaphore s;
    std::atomic<size_t> writers = 0;
    std::atomic<size_t> readers = 0;

    const auto actor = [&](size_t i) {
        return [&, i] {
            TReallyFastRng32 r(i);

            for (size_t i = 0; i < Iterations; ++i) {
                switch (r.Uniform(0ULL, 4ULL)) {
                    case 0:
                    case 1:
                    case 2: {
                        TReadGuardBase _(s);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 0);
                        UNIT_ASSERT_GE(readers.load(), 0);
                        readers.fetch_add(1);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 0);
                        UNIT_ASSERT_GT(readers.load(), 0);
                        readers.fetch_sub(1);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 0);
                        UNIT_ASSERT_GE(readers.load(), 0);
                        break;
                    }
                    case 3: {
                        TWriteGuardBase _(s);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(readers.load(), 0);
                        writers.fetch_add(1);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(readers.load(), 0);
                        writers.fetch_sub(1);
                        UNIT_ASSERT_VALUES_EQUAL(writers.load(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(readers.load(), 0);
                        break;
                    }
                }
            }
        };
    };

    auto pool = CreateThreadPool(/*threadCount=*/Concurrency);
    for (size_t i = 0; i < Concurrency; ++i) {
        pool->SafeAddFunc(actor(i));
    }
}

} // Y_UNIT_TEST_SUITE(RWBinarySemaphoreTests)
