#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <yt/yt/core/concurrency/async_barrier.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <random>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderWriterLockTest
    : public ::testing::Test
{
protected:
    TAsyncReaderWriterLock lock;
};

TEST_F(TAsyncReaderWriterLockTest, ReadMode)
{
    EXPECT_TRUE(lock.AcquireReader().IsSet());
    EXPECT_TRUE(lock.AcquireReader().IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(lock.AcquireReader().IsSet());

    lock.ReleaseReader();
    lock.ReleaseReader();
}

TEST_F(TAsyncReaderWriterLockTest, WriteMode)
{
    EXPECT_TRUE(lock.AcquireWriter().IsSet());

    auto firstFuture = lock.AcquireWriter();
    EXPECT_FALSE(firstFuture.IsSet());

    auto secondFuture = lock.AcquireWriter();
    EXPECT_FALSE(secondFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(secondFuture.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, WriteAfterRead)
{
    YT_UNUSED_FUTURE(lock.AcquireReader());

    auto future = lock.AcquireWriter();
    EXPECT_FALSE(future.IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(future.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, ServeWriteFirst)
{
    YT_UNUSED_FUTURE(lock.AcquireReader());

    auto writeFuture = lock.AcquireWriter();
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = lock.AcquireReader();
    EXPECT_FALSE(readFuture.IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(writeFuture.IsSet());
    EXPECT_FALSE(readFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(readFuture.IsSet());

    lock.ReleaseReader();
}

TEST_F(TAsyncReaderWriterLockTest, CreateReaderGuard)
{
    auto firstGuard = TAsyncLockReaderGuard::Acquire(&lock);
    EXPECT_TRUE(firstGuard.IsSet());

    {
        auto secondGuard = TAsyncLockReaderGuard::Acquire(&lock);
        EXPECT_TRUE(secondGuard.IsSet());
    }

    auto thirdGuard = TAsyncLockReaderGuard::Acquire(&lock);
    EXPECT_TRUE(thirdGuard.IsSet());
}

TEST_F(TAsyncReaderWriterLockTest, CreateWriterGuard)
{
    auto guardFuture = TAsyncLockWriterGuard::Acquire(&lock);
    EXPECT_TRUE(guardFuture.IsSet());

    auto guard = guardFuture.Get().Value();

    auto future = lock.AcquireWriter();
    EXPECT_FALSE(future.IsSet());

    guard->Release();
    EXPECT_TRUE(future.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, ReleaseInDestructor)
{
    TFuture<void> future;
    {
        auto writeGuard = TAsyncLockWriterGuard::Acquire(&lock).Get().Value();
        future = lock.AcquireReader();
        EXPECT_FALSE(future.IsSet());
    }
    EXPECT_TRUE(future.IsSet());
}

template <bool DuringWrite>
void DoTestCancelWriter(TAsyncReaderWriterLock& lock)
{
    auto guard = std::invoke([&] {
        if constexpr (DuringWrite) {
            return TAsyncLockWriterGuard::Acquire(&lock).Get().Value();
        } else {
            return TAsyncLockReaderGuard::Acquire(&lock).Get().Value();
        }
    });

    TAsyncLockWriterGuard::Acquire(&lock)
        .Cancel(TError());

    auto secondGuardFuture = TAsyncLockWriterGuard::Acquire(&lock);

    guard->Release();

    secondGuardFuture
        .WithTimeout(TDuration::Seconds(1))
        .Get()
        .ThrowOnError();
}

TEST_F(TAsyncReaderWriterLockTest, CancelWriterDuringRead)
{
    DoTestCancelWriter<false>(lock);
}

TEST_F(TAsyncReaderWriterLockTest, CancelWriterDuringWrite)
{
    DoTestCancelWriter<true>(lock);
}

TEST_F(TAsyncReaderWriterLockTest, CancelStressTest)
{
    static constexpr auto timeLimit = TDuration::Seconds(10);
    static constexpr int readsPerWrite = 3;
    static constexpr int locksPerCancel = 3;
    static constexpr int threadCount = 5;
    static constexpr int fiberCount = 50;
    static constexpr int maxDelayUs = 50;

    const auto t0 = TInstant::Now();
    const auto seed = std::random_device()();
    Cout << "Seed: " << seed << Endl;

    NThreading::TSpinLock spinlock;
    int readers = 0;
    int writers = 0;
    auto checkInvariants = [&](int deltaReaders, int deltaWriters) {
        auto guard = Guard(spinlock);
        if (writers > 0) {
            EXPECT_EQ(deltaReaders, 0);
            EXPECT_EQ(readers, 0);
            EXPECT_EQ(writers, 1);
        }
        if (readers > 0) {
            EXPECT_EQ(writers, 0);
            EXPECT_EQ(deltaWriters, 0);
        }
        readers += deltaReaders;
        writers += deltaWriters;
    };

    auto barrier = TAsyncBarrier();

    auto task = [&] (std::random_device::result_type seed, TAsyncBarrierCookie cookie) {
        auto tPrev = TInstant::Now();
        auto random = std::mt19937_64(seed);

        auto keepGoing = [&]() -> bool {
            const auto t = TInstant::Now();
            EXPECT_LT(tPrev - t, TDuration::Seconds(1));
            tPrev = t;
            return (t - t0) < timeLimit;
        };

        auto nap = [&]() {
            TDelayedExecutor::WaitForDuration(TDuration::MicroSeconds(
                std::uniform_int_distribution(0, maxDelayUs)(random)));
        };

        while (keepGoing()) {
            [[maybe_unused]] bool shouldCancel = std::uniform_int_distribution(0, locksPerCancel)(random) == 0;
            bool shouldWrite = std::uniform_int_distribution(0, readsPerWrite)(random) == 0;
            if (shouldWrite) {
                nap();
                auto guardFuture = TAsyncLockWriterGuard::Acquire(&lock);
                nap();
                if (shouldCancel && guardFuture.Cancel(TError())) {
                    continue;
                }
                nap();
                auto guard = WaitFor(guardFuture).ValueOrThrow();
                checkInvariants(0, 1);
                nap();
                checkInvariants(0, -1);
            } else {
                nap();
                auto guardFuture = TAsyncLockReaderGuard::Acquire(&lock);
                nap();
                if (shouldCancel && guardFuture.Cancel(TError())) {
                    continue;
                }
                nap();
                auto guard = WaitFor(guardFuture);
                checkInvariants(1, 0);
                nap();
                checkInvariants(-1, 0);
            }
        }

        barrier.Remove(cookie);
    };

    auto threadPool = CreateThreadPool(threadCount, "test");
    auto invoker = threadPool->GetInvoker();

    for (int i = 0; i < fiberCount; ++i) {
        invoker->Invoke(BIND(task, seed + i, barrier.Insert()));
    }

    barrier.GetBarrierFuture().Get().ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
