#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <ydb/library/actors/util/should_continue.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

#define VALUES_EQUAL(a, b, ...) \
        UNIT_ASSERT_VALUES_EQUAL_C((a), (b), (i64)semaphore.OldSemaphore \
                << ' ' << (i64)semaphore.CurrentSleepThreadCount \
                << ' ' << (i64)semaphore.CurrentThreadCount __VA_ARGS__);

Y_UNIT_TEST_SUITE(ExecutorPoolSemaphore) {

    Y_UNIT_TEST(Semaphore) {
        TBasicExecutorPool::TSemaphore semaphore;
        semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(0);

        VALUES_EQUAL(0, semaphore.ConvertToI64());
        semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(-1);
        VALUES_EQUAL(-1, semaphore.ConvertToI64());
        semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(1);
        VALUES_EQUAL(1, semaphore.ConvertToI64());

        for (i64 value = -1'000'000; value <= 1'000'000; ++value) {
            VALUES_EQUAL(TBasicExecutorPool::TSemaphore::GetSemaphore(value).ConvertToI64(), value);
        }

        for (i8 sleepThreads = -10; sleepThreads <= 10; ++sleepThreads) {

            semaphore = TBasicExecutorPool::TSemaphore();
            semaphore.CurrentSleepThreadCount = sleepThreads;
            i64 initialValue = semaphore.ConvertToI64();

            semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(initialValue - 1);
            VALUES_EQUAL(-1, semaphore.OldSemaphore);

            i64 value = initialValue;
            value -= 100;
            for (i32 expected = -100; expected <= 100; ++expected) {
                semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(value);
                UNIT_ASSERT_VALUES_EQUAL_C(expected, semaphore.OldSemaphore, (i64)semaphore.OldSemaphore
                        << ' ' << (i64)semaphore.CurrentSleepThreadCount
                        << ' ' << (i64)semaphore.CurrentThreadCount);
                UNIT_ASSERT_VALUES_EQUAL_C(sleepThreads, semaphore.CurrentSleepThreadCount, (i64)semaphore.OldSemaphore
                        << ' ' << (i64)semaphore.CurrentSleepThreadCount
                        << ' ' << (i64)semaphore.CurrentThreadCount);
                semaphore = TBasicExecutorPool::TSemaphore();
                semaphore.OldSemaphore = expected;
                semaphore.CurrentSleepThreadCount = sleepThreads;
                UNIT_ASSERT_VALUES_EQUAL(semaphore.ConvertToI64(), value);
                value++;
            }

            for (i32 expected = 101; expected >= -101; --expected) {
                semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(value);
                UNIT_ASSERT_VALUES_EQUAL_C(expected, semaphore.OldSemaphore, (i64)semaphore.OldSemaphore
                        << ' ' << (i64)semaphore.CurrentSleepThreadCount
                        << ' ' << (i64)semaphore.CurrentThreadCount);
                UNIT_ASSERT_VALUES_EQUAL_C(sleepThreads, semaphore.CurrentSleepThreadCount, (i64)semaphore.OldSemaphore
                        << ' ' << (i64)semaphore.CurrentSleepThreadCount
                        << ' ' << (i64)semaphore.CurrentThreadCount);
                value--;
            }
        }
    }

} 