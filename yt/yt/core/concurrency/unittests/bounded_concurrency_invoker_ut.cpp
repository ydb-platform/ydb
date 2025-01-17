#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/lazy_ptr.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SleepQuantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TBoundedConcurrencyInvokerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue1;
    TLazyIntrusivePtr<TActionQueue> Queue2;

    void TearDown() override
    {
        if (Queue1.HasValue()) {
            Queue1->Shutdown();
        }
        if (Queue2.HasValue()) {
            Queue2->Shutdown();
        }
    }
};

TEST_F(TBoundedConcurrencyInvokerTest, WaitFor1)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);
    BIND([&] {
        for (int i = 0; i < 10; ++i) {
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
    }).AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TBoundedConcurrencyInvokerTest, WaitFor2)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 2);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto a1 = BIND([&] {
        promise.Set();
    });

    auto a2 = BIND([&] {
        invoker->Invoke(a1);
        WaitFor(future)
            .ThrowOnError();
    });

    a2.AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TBoundedConcurrencyInvokerTest, WaitFor3)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    bool a1called = false;
    bool a1finished = false;
    auto a1 = BIND([&] {
        a1called = true;
        WaitFor(future)
            .ThrowOnError();
        a1finished = true;
    });

    bool a2called = false;
    auto a2 = BIND([&] {
        a2called = true;
    });

    invoker->Invoke(a1);
    invoker->Invoke(a2);

    Sleep(SleepQuantum);
    EXPECT_TRUE(a1called);
    EXPECT_FALSE(a1finished);
    EXPECT_FALSE(a2called);

    promise.Set();

    Sleep(SleepQuantum);
    EXPECT_TRUE(a1called);
    EXPECT_TRUE(a1finished);
    EXPECT_TRUE(a2called);
}

////////////////////////////////////////////////////////////////////////////////

class TBoundedConcurrencyInvokerParametrizedReconfigureTest
    : public TBoundedConcurrencyInvokerTest
    , public ::testing::WithParamInterface<std::tuple<int, int, bool>>
{ };

INSTANTIATE_TEST_SUITE_P(
    Test,
    TBoundedConcurrencyInvokerParametrizedReconfigureTest,
    ::testing::Values(
        std::tuple(3, 5, true),
        std::tuple(5, 3, true),
        std::tuple(3, 5, false),
        std::tuple(5, 3, false)));

TEST_P(TBoundedConcurrencyInvokerParametrizedReconfigureTest, SetMaxConcurrentInvocations)
{
    auto [initialMaxConcurrentInvocations, finalMaxConcurrentInvocations, invokeSecondBatchRightAway] = GetParam();
    int maxConcurrentInvocations = initialMaxConcurrentInvocations;
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), maxConcurrentInvocations);

    // Set firstPromise, then secondPromise.
    auto firstPromise = NewPromise<void>();
    auto secondPromise = NewPromise<void>();

    auto firstFuture = firstPromise.ToFuture();
    auto secondFuture = secondPromise.ToFuture();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, lock);
    int runnedCallbacks = 0;
    int finishedCallbacks = 0;

    std::vector<std::vector<TFuture<void>>> callbacks;

    auto invokeCallbacks = [&] (std::optional<TFuture<void>> startFuture = {}) {
        for (int i = 0; i < 10; i++) {
            callbacks.back().push_back(BIND([
                &,
                callbackIndex = i,
                startFuture
            ] {
                if (startFuture) {
                    WaitFor(*startFuture)
                        .ThrowOnError();
                }

                {
                    auto guard = Guard(lock);
                    runnedCallbacks += 1;
                }

                // Later callbacks wait for the first future to set to check that
                // they are not scheduled before first MaxConcurrentInvocations callbacks.
                WaitFor((callbackIndex > maxConcurrentInvocations)
                    ? firstFuture
                    : secondFuture)
                    .ThrowOnError();

                auto guard = Guard(lock);

                auto concurrentInvocations = runnedCallbacks - finishedCallbacks;
                THROW_ERROR_EXCEPTION_UNLESS(concurrentInvocations <= maxConcurrentInvocations, "Number of concurrent invocations exceeds maximum (ConcurrentInvocations: %v, MaxConcurrentInvocations: %v)",
                    concurrentInvocations,
                    maxConcurrentInvocations);
                if (callbackIndex > maxConcurrentInvocations) {
                    THROW_ERROR_EXCEPTION_UNLESS(finishedCallbacks > maxConcurrentInvocations, "%v-th callback was executed before first %v",
                        callbackIndex + 1, maxConcurrentInvocations);
                }

                finishedCallbacks += 1;
            }).AsyncVia(invoker).Run());
        }
    };

    auto resetState = [&] {
        firstPromise = NewPromise<void>();
        secondPromise = NewPromise<void>();

        firstFuture = firstPromise.ToFuture();
        secondFuture = secondPromise.ToFuture();

        runnedCallbacks = 0;
        finishedCallbacks = 0;
    };

    callbacks.emplace_back();
    invokeCallbacks();

    auto startPromise = NewPromise<void>();
    auto catchUpPromise = NewPromise<void>();
    std::vector<TFuture<void>> catchUpCallbacks;

    auto invokeSecondBatch = [&] {
        // To properly decrease max concurrent invocations we need some buffer callbacks.
        auto catchUpFuture = catchUpPromise.ToFuture();
        if (finalMaxConcurrentInvocations < initialMaxConcurrentInvocations) {
            for (int i = 0; i < 10; i++) {
                catchUpCallbacks.push_back(BIND([catchUpFuture] {
                    WaitFor(catchUpFuture)
                        .ThrowOnError();
                }).AsyncVia(invoker).Run());
            }
        }

        callbacks.emplace_back();
        invokeCallbacks(startPromise.ToFuture());
    };

    if (invokeSecondBatchRightAway) {
        invokeSecondBatch();
    }

    ASSERT_EQ(std::ssize(callbacks), invokeSecondBatchRightAway ? 2 : 1);
    ASSERT_EQ(std::ssize(callbacks[0]), 10);
    if (invokeSecondBatchRightAway) {
        ASSERT_EQ(std::ssize(callbacks[1]), 10);
    }

    firstPromise.Set();
    secondPromise.Set();

    WaitFor(AllSucceeded(callbacks[0]))
        .ThrowOnError();
    EXPECT_EQ(runnedCallbacks, 10);
    EXPECT_EQ(finishedCallbacks, 10);

    resetState();

    maxConcurrentInvocations = finalMaxConcurrentInvocations;
    invoker->SetMaxConcurrentInvocations(maxConcurrentInvocations);

    if (!invokeSecondBatchRightAway) {
        invokeSecondBatch();
    }

    ASSERT_EQ(std::ssize(callbacks), 2);
    ASSERT_EQ(std::ssize(callbacks[1]), 10);

    if (finalMaxConcurrentInvocations < initialMaxConcurrentInvocations) {
        // Wait until pending change is applied
        catchUpPromise.Set();
        WaitFor(AllSucceeded(catchUpCallbacks))
            .ThrowOnError();
    }

    startPromise.Set();

    firstPromise.Set();
    secondPromise.Set();

    WaitFor(AllSucceeded(callbacks[1]))
        .ThrowOnError();
    EXPECT_EQ(runnedCallbacks, 10);
    EXPECT_EQ(finishedCallbacks, 10);
}

TEST_F(TBoundedConcurrencyInvokerTest, ReconfigureBeforeFirstInvocation)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 0);
    invoker->SetMaxConcurrentInvocations(1);

    auto promise = NewPromise<void>();

    BIND([&] {
        promise.Set();
    })
        .AsyncVia(invoker)
        .Run()
        .Get()
        .ThrowOnError();

    EXPECT_TRUE(promise.IsSet());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
