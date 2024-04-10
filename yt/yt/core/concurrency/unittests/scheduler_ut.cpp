#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/current_invoker.h>
// TODO(lukyan): Move invoker_detail to concurrency? Merge concurrency and actions?
#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/config.h>
#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/yt/threading/count_down_latch.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>
#include <util/system/type_name.h>

#include <exception>

namespace NYT::NConcurrency {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTracing;

using ::testing::ContainsRegex;

////////////////////////////////////////////////////////////////////////////////

constexpr auto SleepQuantum = TDuration::MilliSeconds(100);

inline const NLogging::TLogger Logger("SchedulerTest");

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> MakeDelayedFuture(T x)
{
    return TDelayedExecutor::MakeDelayed(SleepQuantum)
        .Apply(BIND([=] () { return x; }));
}

class TSchedulerTest
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

////////////////////////////////////////////////////////////////////////////////

int RecursiveFunction(size_t maxDepth, size_t currentDepth)
{
    if (currentDepth >= maxDepth) {
        return 0;
    }

// It seems that aarch64 requires more stack to throw an exception.
#if defined(__aarch64__) || defined(__arm64__)
    constexpr size_t requiredStackSpace = 60_KB;
#else
    constexpr size_t requiredStackSpace = 40_KB;
#endif

    if (!CheckFreeStackSpace(requiredStackSpace)) {
        THROW_ERROR_EXCEPTION("Evaluation depth causes stack overflow");
    }

    std::array<int, 4 * 1024> array;

    for (size_t i = 0; i < array.size(); ++i) {
        array[i] = rand();
    }

    int result = RecursiveFunction(maxDepth, currentDepth + 1);

    return std::accumulate(array.begin(), array.end(), 0) + result;
}

// Fiber stack base address is unavailable on Windows (and always returns nullptr).
#ifndef _win_
TEST_W(TSchedulerTest, CheckFiberStack)
{
    auto asyncResult1 = BIND(&RecursiveFunction, 10, 0)
        .AsyncVia(Queue1->GetInvoker())
        .Run();

    WaitFor(asyncResult1).ThrowOnError();

#if defined(_asan_enabled_) || defined(_msan_enabled_)
    constexpr size_t tooLargeDepth = 160;
#else
    constexpr size_t tooLargeDepth = 20;
#endif

    auto asyncResult2 = BIND(&RecursiveFunction, tooLargeDepth, 0)
        .AsyncVia(Queue1->GetInvoker())
        .Run();

    EXPECT_THROW_THAT(
        WaitFor(asyncResult2).ThrowOnError(),
        ContainsRegex("Evaluation depth causes stack overflow"));
}
#endif

TEST_W(TSchedulerTest, SimpleAsync)
{
    auto asyncA = MakeDelayedFuture(3);
    int a = WaitFor(asyncA).ValueOrThrow();

    auto asyncB = MakeDelayedFuture(4);
    int b = WaitFor(asyncB).ValueOrThrow();

    EXPECT_EQ(7, a + b);
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

TEST_W(TSchedulerTest, SwitchToInvoker1)
{
    auto invoker = Queue1->GetInvoker();

    auto id0 = GetCurrentThreadId();
    auto id1 = invoker->GetThreadId();

    EXPECT_NE(id0, id1);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker);
        EXPECT_EQ(GetCurrentThreadId(), id1);
    }
}

TEST_W(TSchedulerTest, SwitchToInvoker2)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto id0 = GetCurrentThreadId();
    auto id1 = invoker1->GetThreadId();
    auto id2 = invoker2->GetThreadId();

    EXPECT_NE(id0, id1);
    EXPECT_NE(id0, id2);
    EXPECT_NE(id1, id2);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker1);
        EXPECT_EQ(GetCurrentThreadId(), id1);

        SwitchTo(invoker2);
        EXPECT_EQ(GetCurrentThreadId(), id2);
    }
}

#endif

TEST_W(TSchedulerTest, SwitchToCancelableInvoker1)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetCurrentInvoker());

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, SwitchToCancelableInvoker2)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = context->CreateInvoker(Queue2->GetInvoker());

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker2); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, SwitchToCancelableInvoker3)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = context->CreateInvoker(Queue2->GetInvoker());

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    EXPECT_NO_THROW({ SwitchTo(invoker2); });

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker2); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, WaitForCancelableInvoker1)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    TDelayedExecutor::Submit(
        BIND([=] () mutable {
            context->Cancel(TError("Error"));
            promise.Set();
        }),
        SleepQuantum);
    WaitFor(BIND([=] () {
            EXPECT_THROW({ WaitFor(future).ThrowOnError(); }, TFiberCanceledException);
        })
        .AsyncVia(invoker)
        .Run()).ThrowOnError();
}

TEST_W(TSchedulerTest, WaitForCancelableInvoker2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    WaitFor(BIND([=] () mutable {
            context->Cancel(TError("Error"));
            promise.Set();
            EXPECT_THROW({ WaitFor(future).ThrowOnError(); }, TFiberCanceledException);
        })
        .AsyncVia(invoker)
        .Run()).ThrowOnError();
}

TEST_W(TSchedulerTest, TerminatedCaught)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker1); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, TerminatedPropagated)
{
    TLazyIntrusivePtr<TActionQueue> queue3;

    auto future = BIND([this] {
        auto context = New<TCancelableContext>();

        auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
        auto invoker2 = Queue2->GetInvoker();

        SwitchTo(invoker2);

        context->Cancel(TError("Error"));

        SwitchTo(invoker1);

        YT_ABORT();
    })
    .AsyncVia(queue3->GetInvoker())
    .Run();

    EXPECT_THROW_WITH_ERROR_CODE(WaitFor(future).ThrowOnError(), NYT::EErrorCode::Canceled);
}

TEST_W(TSchedulerTest, CurrentInvokerAfterSwitch1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

TEST_W(TSchedulerTest, CurrentInvokerAfterSwitch2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

TEST_W(TSchedulerTest, InvokerAffinity1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    VERIFY_INVOKER_AFFINITY(invoker);
}

TEST_W(TSchedulerTest, InvokerAffinity2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    VERIFY_INVOKER_AFFINITY(invoker);
    VERIFY_INVOKER_AFFINITY(Queue1->GetInvoker());
}

using TSchedulerDeathTest = TSchedulerTest;

TEST_W(TSchedulerDeathTest, SerializedInvokerAffinity)
{
    auto actionQueueInvoker = Queue1->GetInvoker();

    auto threadPool = CreateThreadPool(4, "TestThreads");
    auto threadPoolInvoker = threadPool->GetInvoker();
    auto serializedThreadPoolInvoker = CreateSerializedInvoker(threadPoolInvoker);

    SwitchTo(actionQueueInvoker);

    VERIFY_INVOKER_AFFINITY(actionQueueInvoker);
    VERIFY_SERIALIZED_INVOKER_AFFINITY(actionQueueInvoker);

    SwitchTo(serializedThreadPoolInvoker);

    VERIFY_INVOKER_AFFINITY(serializedThreadPoolInvoker);
    VERIFY_SERIALIZED_INVOKER_AFFINITY(serializedThreadPoolInvoker);

    SwitchTo(threadPoolInvoker);

    VERIFY_INVOKER_AFFINITY(threadPoolInvoker);
    ASSERT_DEATH({ VERIFY_SERIALIZED_INVOKER_AFFINITY(threadPoolInvoker); }, ".*");
}

#endif

TEST_F(TSchedulerTest, CurrentInvokerSync)
{
    auto currentInvokerPtr = GetCurrentInvoker();
    const auto& currentInvoker = *currentInvokerPtr;
    EXPECT_EQ(GetSyncInvoker(), currentInvokerPtr)
        << "Current invoker: " << TypeName(currentInvoker);
}

TEST_F(TSchedulerTest, CurrentInvokerInActionQueue)
{
    auto invoker = Queue1->GetInvoker();
    BIND([=] () {
        EXPECT_EQ(invoker, GetCurrentInvoker());
    })
    .AsyncVia(invoker).Run()
    .Get();
}

TEST_F(TSchedulerTest, Intercept)
{
    auto invoker = Queue1->GetInvoker();
    int counter1 = 0;
    int counter2 = 0;
    BIND([&] () {
        TContextSwitchGuard guard(
            [&] {
                EXPECT_EQ(counter1, 0);
                EXPECT_EQ(counter2, 0);
                ++counter1;
            },
            [&] {
                EXPECT_EQ(counter1, 1);
                EXPECT_EQ(counter2, 0);
                ++counter2;
            });
        TDelayedExecutor::WaitForDuration(SleepQuantum);
    })
    .AsyncVia(invoker).Run()
    .Get();
    EXPECT_EQ(counter1, 1);
    EXPECT_EQ(counter2, 1);
}

TEST_F(TSchedulerTest, InterceptEnclosed)
{
    auto invoker = Queue1->GetInvoker();
    int counter1 = 0;
    int counter2 = 0;
    int counter3 = 0;
    int counter4 = 0;
    BIND([&] () {
        {
            TContextSwitchGuard guard(
                [&] { ++counter1; },
                [&] { ++counter2; });
            TDelayedExecutor::WaitForDuration(SleepQuantum);
            {
                TContextSwitchGuard guard2(
                    [&] { ++counter3; },
                    [&] { ++counter4; });
                TDelayedExecutor::WaitForDuration(SleepQuantum);
            }
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
        TDelayedExecutor::WaitForDuration(SleepQuantum);
    })
    .AsyncVia(invoker).Run()
    .Get();
    EXPECT_EQ(counter1, 3);
    EXPECT_EQ(counter2, 3);
    EXPECT_EQ(counter3, 1);
    EXPECT_EQ(counter4, 1);
}

TEST_F(TSchedulerTest, CurrentInvokerConcurrent)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto result1 = BIND([=] () {
        EXPECT_EQ(invoker1, GetCurrentInvoker());
    }).AsyncVia(invoker1).Run();

    auto result2 = BIND([=] () {
        EXPECT_EQ(invoker2, GetCurrentInvoker());
    }).AsyncVia(invoker2).Run();

    result1.Get();
    result2.Get();
}

TEST_W(TSchedulerTest, WaitForAsyncVia)
{
    auto invoker = Queue1->GetInvoker();

    auto x = BIND([&] () { }).AsyncVia(invoker).Run();

    WaitFor(x)
        .ThrowOnError();
}

// Various invokers.

TEST_F(TSchedulerTest, WaitForInSerializedInvoker1)
{
    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
    }).AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInSerializedInvoker2)
{
    // NB! This may be confusing, but serialized invoker is expected to start
    // executing next action if current action is blocked on WaitFor.

    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    std::vector<TFuture<void>> futures;

    bool finishedFirstAction = false;
    futures.emplace_back(BIND([&] () {
        TDelayedExecutor::WaitForDuration(SleepQuantum);
        finishedFirstAction = true;
    }).AsyncVia(invoker).Run());

    futures.emplace_back(BIND([&] () {
        if (finishedFirstAction) {
            THROW_ERROR_EXCEPTION("Serialization error");
        }
    }).AsyncVia(invoker).Run());

    AllSucceeded(futures).Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker1)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
    }).AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker2)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 2);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto a1 = BIND([&] () {
        promise.Set();
    });

    auto a2 = BIND([&] () {
        invoker->Invoke(a1);
        WaitFor(future)
            .ThrowOnError();
    });

    a2.AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker3)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    bool a1called = false;
    bool a1finished = false;
    auto a1 = BIND([&] () {
        a1called = true;
        WaitFor(future)
            .ThrowOnError();
        a1finished = true;
    });

    bool a2called = false;
    auto a2 = BIND([&] () {
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

TEST_F(TSchedulerTest, PropagateFiberCancelationToFuture)
{
    auto p1 = NewPromise<void>();
    auto f1 = p1.ToFuture();

    auto a = BIND([=] () mutable {
        WaitUntilSet(f1);
    });

    auto f2 = a.AsyncVia(Queue1->GetInvoker()).Run();

    Sleep(SleepQuantum);

    f2.Cancel(TError("Error"));

    Sleep(SleepQuantum);

    EXPECT_TRUE(p1.IsCanceled());
}

TEST_F(TSchedulerTest, FiberUnwindOrder)
{
    auto p1 = NewPromise<void>();
    // Add empty callback
    p1.OnCanceled(BIND([] (const TError& /*error*/) { }));
    auto f1 = p1.ToFuture();

    auto f2 = BIND([=] () mutable {
        auto finally = Finally([&] {
            EXPECT_TRUE(f1.IsSet());
        });

        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));

        WaitUntilSet(f1);
    }).AsyncVia(Queue1->GetInvoker()).Run();

    Sleep(SleepQuantum);
    EXPECT_FALSE(f2.IsSet());

    p1.Set();
    Sleep(SleepQuantum);
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST_F(TSchedulerTest, TestWaitUntilSet)
{
    auto p1 = NewPromise<void>();
    auto f1 = p1.ToFuture();

    YT_UNUSED_FUTURE(BIND([=] () {
        Sleep(SleepQuantum);
        p1.Set();
    }).AsyncVia(Queue1->GetInvoker()).Run());

    WaitUntilSet(f1);
    EXPECT_TRUE(f1.IsSet());
    EXPECT_TRUE(f1.Get().IsOK());
}

TEST_F(TSchedulerTest, AsyncViaCanceledBeforeStart)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([] () {
        Sleep(SleepQuantum * 10);
    }).AsyncVia(invoker).Run();
    auto asyncResult2 = BIND([] () {
        Sleep(SleepQuantum * 10);
    }).AsyncVia(invoker).Run();
    EXPECT_FALSE(asyncResult1.IsSet());
    EXPECT_FALSE(asyncResult2.IsSet());
    asyncResult2.Cancel(TError("Error"));
    EXPECT_TRUE(asyncResult1.Get().IsOK());
    Sleep(SleepQuantum);
    EXPECT_TRUE(asyncResult2.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, asyncResult2.Get().GetCode());
}

TEST_F(TSchedulerTest, CancelCurrentFiber)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult = BIND([=] () {
        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
        SwitchTo(invoker);
    }).AsyncVia(invoker).Run();
    asyncResult.Get();
    EXPECT_TRUE(asyncResult.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, asyncResult.Get().GetCode());
}

TEST_F(TSchedulerTest, YieldToFromCanceledFiber)
{
    auto promise = NewPromise<void>();
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto asyncResult = BIND([=] () mutable {
        BIND([=] () {
            NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
        }).AsyncVia(invoker2).Run().Get();
        WaitFor(promise.ToFuture(), invoker2)
            .ThrowOnError();
    }).AsyncVia(invoker1).Run();

    promise.Set();
    asyncResult.Get();

    EXPECT_TRUE(asyncResult.IsSet());
    EXPECT_TRUE(asyncResult.Get().IsOK());
}

TEST_F(TSchedulerTest, JustYield1)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult = BIND([] () {
        for (int i = 0; i < 10; ++i) {
            Yield();
        }
    }).AsyncVia(invoker).Run().Get();
    EXPECT_TRUE(asyncResult.IsOK());
}

TEST_F(TSchedulerTest, JustYield2)
{
    auto invoker = Queue1->GetInvoker();

    bool flag = false;

    auto asyncResult = BIND([&] () {
        for (int i = 0; i < 2; ++i) {
            Sleep(SleepQuantum);
            Yield();
        }
        flag = true;
    }).AsyncVia(invoker).Run();

    // This callback must complete before the first.
    auto errorOrValue = BIND([&] () {
        return flag;
    }).AsyncVia(invoker).Run().Get();

    EXPECT_TRUE(errorOrValue.IsOK());
    EXPECT_FALSE(errorOrValue.Value());
    EXPECT_TRUE(asyncResult.Get().IsOK());
}

TEST_F(TSchedulerTest, CancelInAdjacentCallback)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([=] () {
        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
    }).AsyncVia(invoker).Run().Get();
    auto asyncResult2 = BIND([=] () {
        Yield();
    }).AsyncVia(invoker).Run().Get();
    EXPECT_TRUE(asyncResult1.IsOK());
    EXPECT_TRUE(asyncResult2.IsOK());
}

TEST_F(TSchedulerTest, CancelInApply)
{
    auto invoker = Queue1->GetInvoker();

    BIND([=] () {
        auto promise = NewPromise<void>();

        YT_UNUSED_FUTURE(promise.ToFuture().Apply(BIND([] {
            auto canceler = NYT::NConcurrency::GetCurrentFiberCanceler();
            canceler(TError("kek"));

            auto p = NewPromise<void>();
            WaitFor(p.ToFuture())
                .ThrowOnError();
        })));

        promise.Set();

        YT_UNUSED_FUTURE(promise.ToFuture().Apply(BIND([] {
            auto canceler = NYT::NConcurrency::GetCurrentFiberCanceler();
            canceler(TError("kek"));

            auto p = NewPromise<void>();
            WaitFor(p.ToFuture())
                .ThrowOnError();
        })));
    })
        .AsyncVia(invoker)
        .Run()
        .Get();
}

TEST_F(TSchedulerTest, CancelInApplyUnique)
{
    auto invoker = Queue1->GetInvoker();

    BIND([=] () {
        auto promise = NewPromise<int>();

        auto f2 = promise.ToFuture().ApplyUnique(BIND([] (TErrorOr<int>&& /*error*/) {
            auto canceler = NYT::NConcurrency::GetCurrentFiberCanceler();
            canceler(TError("kek"));

            auto p = NewPromise<void>();
            WaitFor(p.ToFuture())
                .ThrowOnError();
        }));

        promise.Set(42);
    })
        .AsyncVia(invoker)
        .Run()
        .Get();
}

TEST_F(TSchedulerTest, CancelInAdjacentThread)
{
    auto closure = TCallback<void(const TError&)>();
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([=, &closure] () {
        closure = NYT::NConcurrency::GetCurrentFiberCanceler();
    }).AsyncVia(invoker).Run().Get();
    closure.Run(TError("Error")); // *evil laugh*
    auto asyncResult2 = BIND([=] () {
        Yield();
    }).AsyncVia(invoker).Run().Get();
    closure.Reset(); // *evil smile*
    EXPECT_TRUE(asyncResult1.IsOK());
    EXPECT_TRUE(asyncResult2.IsOK());
}

TEST_F(TSchedulerTest, SerializedDoubleWaitFor)
{
    std::atomic<bool> flag(false);

    auto threadPool = CreateThreadPool(3, "MyPool");
    auto serializedInvoker = CreateSerializedInvoker(threadPool->GetInvoker());

    auto promise = NewPromise<void>();

    BIND([&] () {
        WaitFor(VoidFuture)
            .ThrowOnError();
        WaitFor(VoidFuture)
            .ThrowOnError();
        promise.Set();

        Sleep(SleepQuantum);
        flag = true;
    })
    .Via(serializedInvoker)
    .Run();

    promise.ToFuture().Get();

    auto result = BIND([&] () -> bool {
        return flag;
    })
    .AsyncVia(serializedInvoker)
    .Run()
    .Get()
    .ValueOrThrow();

    EXPECT_TRUE(result);
}

void CheckCurrentFiberRunDuration(TDuration actual, TDuration lo, TDuration hi)
{
    EXPECT_LE(actual, hi);
    EXPECT_GE(actual, lo);
}

TEST_W(TSchedulerTest, FiberTiming)
{
    NProfiling::TFiberWallTimer timer;

    CheckCurrentFiberRunDuration(timer.GetElapsedTime(), TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
    Sleep(TDuration::Seconds(1));
    CheckCurrentFiberRunDuration(timer.GetElapsedTime(), TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    CheckCurrentFiberRunDuration(timer.GetElapsedTime(), TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

TEST_W(TSchedulerTest, CancelDelayedFuture)
{
    auto future = TDelayedExecutor::MakeDelayed(TDuration::Seconds(10));
    future.Cancel(TError("Error"));
    EXPECT_TRUE(future.IsSet());
    auto error = future.Get();
    EXPECT_EQ(NYT::EErrorCode::Canceled, error.GetCode());
    EXPECT_EQ(1, std::ssize(error.InnerErrors()));
    EXPECT_EQ(NYT::EErrorCode::Generic, error.InnerErrors()[0].GetCode());
}

class TVerifyingMemoryTagGuard
{
public:
    explicit TVerifyingMemoryTagGuard(TMemoryTag tag)
        : Tag_(tag)
        , SavedTag_(GetCurrentMemoryTag())
    {
        SetCurrentMemoryTag(Tag_);
    }

    ~TVerifyingMemoryTagGuard()
    {
        auto tag = GetCurrentMemoryTag();
        EXPECT_EQ(tag, Tag_);
        SetCurrentMemoryTag(SavedTag_);
    }

    TVerifyingMemoryTagGuard(const TVerifyingMemoryTagGuard& other) = delete;
    TVerifyingMemoryTagGuard(TVerifyingMemoryTagGuard&& other) = delete;

private:
    const TMemoryTag Tag_;
    const TMemoryTag SavedTag_;
};

class TWrappingInvoker
    : public TInvokerWrapper
{
public:
    explicit TWrappingInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND(
            &TWrappingInvoker::RunCallback,
            MakeStrong(this),
            Passed(std::move(callback))));
    }

    void RunCallback(TClosure callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        DoRunCallback(callback);
    }

    void virtual DoRunCallback(TClosure callback) = 0;
};

class TVerifyingMemoryTaggingInvoker
    : public TWrappingInvoker
{
public:
    TVerifyingMemoryTaggingInvoker(IInvokerPtr invoker, TMemoryTag memoryTag)
        : TWrappingInvoker(std::move(invoker))
        , MemoryTag_(memoryTag)
    { }

private:
    const TMemoryTag MemoryTag_;

    void DoRunCallback(TClosure callback) override
    {
        TVerifyingMemoryTagGuard memoryTagGuard(MemoryTag_);
        callback();
    }
};

TEST_W(TSchedulerTest, MemoryTagAndResumer)
{
    auto actionQueue = New<TActionQueue>();

    auto invoker1 = New<TVerifyingMemoryTaggingInvoker>(actionQueue->GetInvoker(), 1);
    auto invoker2 = New<TVerifyingMemoryTaggingInvoker>(actionQueue->GetInvoker(), 2);

    auto asyncResult = BIND([=] {
        EXPECT_EQ(GetCurrentMemoryTag(), 1u);
        SwitchTo(invoker2);
        EXPECT_EQ(GetCurrentMemoryTag(), 1u);
    })
        .AsyncVia(invoker1)
        .Run();

    WaitFor(asyncResult)
        .ThrowOnError();
}

void CheckTraceContextTime(const NTracing::TTraceContextPtr& traceContext, TDuration lo, TDuration hi)
{
    auto actual = traceContext->GetElapsedTime();
    EXPECT_LE(actual, hi);
    EXPECT_GE(actual, lo);
}

TEST_W(TSchedulerTest, TraceContextZeroTiming)
{
    auto traceContext = NTracing::TTraceContext::NewRoot("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        Sleep(TDuration::Seconds(0));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
}

TEST_W(TSchedulerTest, TraceContextThreadSleepTiming)
{
    auto traceContext = NTracing::TTraceContext::NewRoot("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        Sleep(TDuration::Seconds(1));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

TEST_W(TSchedulerTest, TraceContextFiberSleepTiming)
{
    auto traceContext = NTracing::TTraceContext::NewRoot("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
}

TEST_W(TSchedulerTest, TraceContextTimingPropagationViaBind)
{
    auto traceContext = NTracing::TTraceContext::NewRoot("Test");
    auto actionQueue = New<TActionQueue>();

    {
        NTracing::TTraceContextGuard guard(traceContext);
        auto asyncResult = BIND([] {
            Sleep(TDuration::MilliSeconds(700));
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run();
        Sleep(TDuration::MilliSeconds(300));
        WaitFor(asyncResult)
            .ThrowOnError();
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

TEST_W(TSchedulerTest, TraceBaggagePropagation)
{
    auto traceContext = TTraceContext::NewRoot("Test");
    auto baggage = CreateEphemeralAttributes();
    baggage->Set("myKey", "myValue");
    baggage->Set("myKey2", "myValue2");
    auto expected = ConvertToYsonString(baggage);
    traceContext->PackBaggage(baggage);

    auto childContext = traceContext->CreateChild("Child");
    auto childBaggage = childContext->UnpackBaggage();
    auto result = ConvertToYsonString(childBaggage);
    childBaggage->Set("myKey3", "myValue3");
    childContext->PackBaggage(std::move(childBaggage));

    EXPECT_EQ(expected, result);
}

TEST_W(TSchedulerTest, TraceDisableSendBaggage)
{
    auto parentContext = TTraceContext::NewRoot("Test");
    auto parentBaggage = CreateEphemeralAttributes();
    parentBaggage->Set("myKey", "myValue");
    parentBaggage->Set("myKey2", "myValue2");
    parentContext->PackBaggage(parentBaggage);
    auto parentBaggageString = ConvertToYsonString(parentBaggage);

    auto originalConfig = GetTracingTransportConfig();
    auto guard = Finally([&] {
        SetTracingTransportConfig(originalConfig);
    });

    {
        auto config = New<TTracingTransportConfig>();
        config->SendBaggage = true;
        SetTracingTransportConfig(std::move(config));
        NTracing::NProto::TTracingExt tracingExt;
        ToProto(&tracingExt, parentContext);
        auto traceContext = TTraceContext::NewChildFromRpc(tracingExt, "Span");
        auto baggage = traceContext->UnpackBaggage();
        ASSERT_NE(baggage, nullptr);
        EXPECT_EQ(ConvertToYsonString(baggage), parentBaggageString);
    }

    {
        auto config = New<TTracingTransportConfig>();
        config->SendBaggage = false;
        SetTracingTransportConfig(std::move(config));
        NTracing::NProto::TTracingExt tracingExt;
        ToProto(&tracingExt, parentContext);
        auto traceContext = TTraceContext::NewChildFromRpc(tracingExt, "Span");
        EXPECT_EQ(traceContext->UnpackBaggage(), nullptr);
    }
}

TEST_W(TSchedulerTest, WaitForFast1)
{
    auto future = MakeFuture<int>(123);
    TContextSwitchGuard guard(
        [] { EXPECT_FALSE(true);},
        nullptr);
    auto value = WaitForFast(future)
        .ValueOrThrow();
    EXPECT_EQ(123, value);
}

TEST_W(TSchedulerTest, WaitForFast2)
{
    auto future = TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100))
        .Apply(BIND([] { return 123; }));
    auto switched = std::make_shared<bool>();
    TContextSwitchGuard guard(
        [=] { *switched = true;},
        nullptr);
    auto value = WaitForFast(future)
        .ValueOrThrow();
    EXPECT_EQ(123, value);
    EXPECT_TRUE(*switched);
}

TEST_W(TSchedulerTest, FutureUpdatedRaceInWaitFor_YT_18899)
{
    auto threadPool = CreateThreadPool(1, "TestThreads");
    auto threadPoolInvoker = threadPool->GetInvoker();
    auto serializedInvoker = CreateSerializedInvoker(threadPoolInvoker);

    // N.B. We are testing race, so make a bunch of reps here.
    for (int i = 0; i != 1'000; ++i) {
        auto promise = NewPromise<void>();
        auto modifiedFuture = promise.ToFuture();

        YT_UNUSED_FUTURE(modifiedFuture.Apply(
            BIND([&] {
                modifiedFuture = MakeFuture(TError{"error that should not be seen"});
            })
                .AsyncVia(serializedInvoker)
        ));

        NThreading::TCountDownLatch latch{1};

        auto testResultFuture = BIND([&] {
            latch.CountDown();
            // N.B. `future` object will be modified after we enter `WaitFor` function.
            // We expect to get result of original future that will succeed.
            WaitFor(modifiedFuture)
                .ThrowOnError();
        })
            .AsyncVia(serializedInvoker)
            .Run();

        // Wait until serialized executor starts executing action.
        latch.Wait();

        YT_UNUSED_FUTURE(BIND([&] {
            // N.B. waiting action is inside WairFor now, because:
            //   - we know that waiting action had started execution before this action was scheduled
            //   - this action is executed inside the same serialized invoker.
            promise.Set();
        })
            .AsyncVia(serializedInvoker)
            .Run());

        ASSERT_NO_THROW(testResultFuture
            .Get()
            .ThrowOnError());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSuspendableInvokerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue1;

    void TearDown() override
    {
        if (Queue1.HasValue()) {
            Queue1->Shutdown();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSuspendableInvokerTest, PollSuspendFuture)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum * 10);
        flag = true;
    })
    .Via(suspendableInvoker)
    .Run();

    auto future = suspendableInvoker->Suspend();

    while (!flag) {
        EXPECT_EQ(flag, future.IsSet());
        Sleep(SleepQuantum);
    }
    Sleep(SleepQuantum);
    EXPECT_EQ(flag, future.IsSet());
}

TEST_F(TSuspendableInvokerTest, SuspendableDoubleWaitFor)
{
    std::atomic<bool> flag(false);

    auto threadPool = CreateThreadPool(3, "MyPool");
    auto suspendableInvoker = CreateSuspendableInvoker(threadPool->GetInvoker());

    auto promise = NewPromise<void>();

    auto setFlagFuture = BIND([&] () {
        WaitFor(VoidFuture)
            .ThrowOnError();
        Sleep(SleepQuantum);
        WaitFor(VoidFuture)
            .ThrowOnError();
        promise.Set();

        Sleep(SleepQuantum * 10);
        flag = true;
    })
    .AsyncVia(suspendableInvoker)
    .Run();

    suspendableInvoker->Suspend().Get();
    EXPECT_FALSE(promise.ToFuture().IsSet());
    suspendableInvoker->Resume();
    promise.ToFuture().Get();

    setFlagFuture.Get();

    auto flagValue = BIND([&] () -> bool {
        return flag;
    })
    .AsyncVia(suspendableInvoker)
    .Run()
    .Get()
    .ValueOrThrow();

    EXPECT_TRUE(flagValue);
}

TEST_F(TSuspendableInvokerTest, EarlySuspend)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    suspendableInvoker->Suspend().Get();

    auto promise = NewPromise<void>();

    BIND([&] () {
        promise.Set();
    })
    .Via(suspendableInvoker)
    .Run();

    EXPECT_FALSE(promise.IsSet());
    suspendableInvoker->Resume();
    promise.ToFuture().Get();
    EXPECT_TRUE(promise.IsSet());
}

TEST_F(TSuspendableInvokerTest, ResumeBeforeFullSuspend)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum);
    })
    .Via(suspendableInvoker)
    .Run();

    auto firstFuture = suspendableInvoker->Suspend();

    EXPECT_FALSE(firstFuture.IsSet());
    suspendableInvoker->Resume();
    EXPECT_FALSE(firstFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, AllowSuspendOnContextSwitch)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto setFlagFuture = BIND([&] () {
        Sleep(SleepQuantum);
        WaitUntilSet(future);
        flag = true;
    })
    .AsyncVia(suspendableInvoker)
    .Run();

    auto suspendFuture = suspendableInvoker->Suspend();
    EXPECT_FALSE(suspendFuture.IsSet());
    EXPECT_TRUE(suspendFuture.Get().IsOK());

    suspendableInvoker->Resume();
    promise.Set();
    setFlagFuture.Get();
    EXPECT_TRUE(flag);
}

TEST_F(TSuspendableInvokerTest, SuspendResumeOnFinishedRace)
{
    std::atomic<bool> flag(false);
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        for (int i = 0; i < 100; ++i) {
            Sleep(TDuration::MilliSeconds(1));
            Yield();
        }
    })
    .Via(suspendableInvoker)
    .Run();

    int hits = 0;
    while (hits < 100) {
        flag = false;
        auto future = suspendableInvoker->Suspend()
            .Apply(BIND([=, &flag] () { flag = true; }));

        if (future.IsSet()) {
            ++hits;
        }
        suspendableInvoker->Resume();
        auto error = future.Get();
        if (!error.IsOK()) {
            EXPECT_FALSE(flag);
            YT_VERIFY(error.GetCode() == NYT::EErrorCode::Canceled);
        } else {
            EXPECT_TRUE(flag);
        }
    }
    auto future = suspendableInvoker->Suspend();
    EXPECT_TRUE(future.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, ResumeInApply)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum);
    })
    .Via(suspendableInvoker)
    .Run();

    auto suspendFuture = suspendableInvoker->Suspend()
        .Apply(BIND([=] () { suspendableInvoker->Resume(); }));

    EXPECT_TRUE(suspendFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, VerifySerializedActionsOrder)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    suspendableInvoker->Suspend()
        .Get();

    const int totalActionCount = 100000;

    std::atomic<int> actionIndex = {0};
    std::atomic<int> reorderingCount = {0};

    for (int i = 0; i < totalActionCount / 2; ++i) {
        BIND([&actionIndex, &reorderingCount, i] () {
            reorderingCount += (actionIndex != i);
            ++actionIndex;
        })
        .Via(suspendableInvoker)
        .Run();
    }

    TDelayedExecutor::Submit(
        BIND([&] () {
            suspendableInvoker->Resume();
        }),
        SleepQuantum / 10);

    for (int i = totalActionCount / 2; i < totalActionCount; ++i) {
        BIND([&actionIndex, &reorderingCount, i] () {
            reorderingCount += (actionIndex != i);
            ++actionIndex;
        })
        .Via(suspendableInvoker)
        .Run();
    }

    while (actionIndex < totalActionCount) {
        Sleep(SleepQuantum);
    }
    EXPECT_EQ(actionIndex, totalActionCount);
    EXPECT_EQ(reorderingCount, 0);
}

////////////////////////////////////////////////////////////////////////////////

class TFairShareSchedulerTest
    : public TSchedulerTest
    , public ::testing::WithParamInterface<std::tuple<int, int, int, TDuration>>
{ };

bool ApproximatelyEqual(TDuration lhs, TDuration rhs, TDuration eps = TDuration::MilliSeconds(1))
{
    return (lhs < rhs ? rhs - lhs : lhs - rhs) < eps;
}

TEST_P(TFairShareSchedulerTest, TwoLevelFairness)
{
    size_t numThreads = std::get<0>(GetParam());
    size_t numWorkers = std::get<1>(GetParam());
    size_t numPools = std::get<2>(GetParam());
    auto work = std::get<3>(GetParam());


    YT_VERIFY(numWorkers > 0);
    YT_VERIFY(numThreads > 0);
    YT_VERIFY(numPools > 0);
    YT_VERIFY(numWorkers > numPools);
    YT_VERIFY(numThreads <= numWorkers);

    auto threadPool = CreateNewTwoLevelFairShareThreadPool(
        numThreads,
        "MyFairSharePool",
        {
            /*poolWeightProvider*/ nullptr,
            /*verboseLogging*/ true
        });

    std::vector<TDuration> progresses(numWorkers);
    std::vector<TDuration> pools(numPools);

    auto getShift = [&] (size_t id) {
        return SleepQuantum * (id + 1) / numWorkers;
    };

    std::vector<TFuture<void>> futures;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, lock);

    for (size_t id = 0; id < numWorkers; ++id) {
        auto invoker = threadPool->GetInvoker(Format("pool%v", id % numPools), Format("worker%v", id));
        auto worker = [&, id] () mutable {
            auto startInstant = GetCpuInstant();

            auto poolId = id % numPools;
            auto initialShift = getShift(id);

            Sleep(initialShift);

            {
                auto guard = Guard(lock);
                auto elapsedTime = CpuDurationToDuration(GetCpuInstant() - startInstant);
                pools[poolId] += elapsedTime;
                progresses[id] += elapsedTime;
            }

            Yield();

            while (progresses[id] < work + initialShift) {
                auto startInstant = GetCpuInstant();

                {
                    auto guard = Guard(lock);

                    if (numThreads == 1) {
                        auto minPool = TDuration::Max();
                        for (size_t index = 0; index < numPools; ++index) {
                            bool hasBucketsInPool = false;
                            for (size_t workerId = index; workerId < numWorkers; workerId += numPools) {
                                if (progresses[workerId] < work + getShift(workerId)) {
                                    hasBucketsInPool = true;
                                }
                            }
                            if (hasBucketsInPool && pools[index] < minPool) {
                                minPool = pools[index];
                            }
                        }

                        YT_LOG_DEBUG("Pools time: %v", pools);
                        YT_LOG_DEBUG("Progresses time: %v", progresses);

                        EXPECT_TRUE(ApproximatelyEqual(pools[poolId], minPool));

                        auto minProgress = TDuration::Max();
                        for (size_t index = poolId; index < numWorkers; index += numPools) {
                            if (progresses[index] < minProgress && progresses[index] < work + getShift(index)) {
                                minProgress = progresses[index];
                            }
                        }

                        EXPECT_TRUE(ApproximatelyEqual(progresses[id], minProgress));
                    }
                }

                Sleep(SleepQuantum * (id + numWorkers) / numWorkers);

                {
                    auto guard = Guard(lock);
                    auto elapsedTime = CpuDurationToDuration(GetCpuInstant() - startInstant);
                    pools[poolId] += elapsedTime;
                    progresses[id] += elapsedTime;
                }

                Yield();
            }
        };

        auto result = BIND(worker)
            .AsyncVia(invoker)
            .Run();

        futures.push_back(result);

        // Random stuck.
        if (id == 1) {
            Sleep(TDuration::MilliSeconds(3));
        }
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
}

TEST_P(TFairShareSchedulerTest, Fairness)
{
    size_t numThreads = std::get<0>(GetParam());
    size_t numWorkers = std::get<1>(GetParam());
    size_t numPools = std::get<2>(GetParam());
    auto work = std::get<3>(GetParam());


    YT_VERIFY(numWorkers > 0);
    YT_VERIFY(numThreads > 0);
    YT_VERIFY(numPools > 0);
    YT_VERIFY(numWorkers > numPools);
    YT_VERIFY(numThreads <= numWorkers);

    if (numPools != 1) {
        return;
    }

    auto threadPool = CreateFairShareThreadPool(numThreads, "MyFairSharePool");

    std::vector<TDuration> progresses(numWorkers);

    auto getShift = [&] (size_t id) {
        return SleepQuantum * (id + 1) / numWorkers;
    };

    std::vector<TFuture<void>> futures;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, lock);

    for (size_t id = 0; id < numWorkers; ++id) {
        auto invoker = threadPool->GetInvoker(Format("worker%v", id));
        auto worker = [&, id] () mutable {
            auto startInstant = GetCpuInstant();

            auto initialShift = getShift(id);

            Sleep(initialShift);

            {
                auto guard = Guard(lock);
                auto elapsedTime = CpuDurationToDuration(GetCpuInstant() - startInstant);
                progresses[id] += elapsedTime;
            }

            Yield();

            while (progresses[id] < work + initialShift) {
                auto startInstant = GetCpuInstant();

                {
                    auto guard = Guard(lock);

                    if (numThreads == 1) {
                        YT_LOG_DEBUG("Progresses time: %v", progresses);

                        auto minProgress = TDuration::Max();
                        for (size_t id = 0; id < numWorkers; ++id) {
                            if (progresses[id] < minProgress && progresses[id] < work + getShift(id)) {
                                minProgress = progresses[id];
                            }
                        }

                        EXPECT_TRUE(ApproximatelyEqual(progresses[id], minProgress));
                    }
                }

                Sleep(SleepQuantum * (id + numWorkers) / numWorkers);

                {
                    auto guard = Guard(lock);
                    auto elapsedTime = CpuDurationToDuration(GetCpuInstant() - startInstant);
                    progresses[id] += elapsedTime;
                }

                Yield();
            }
        };

        auto result = BIND(worker)
            .AsyncVia(invoker)
            .Run();

        futures.push_back(result);
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
}

const auto FSWorkTime = SleepQuantum * 10;

INSTANTIATE_TEST_SUITE_P(
    Test,
    TFairShareSchedulerTest,
    ::testing::Values(
        std::tuple(1, 5, 1, FSWorkTime),
        std::tuple(1, 7, 3, FSWorkTime),
        std::tuple(5, 7, 1, FSWorkTime),
        std::tuple(5, 7, 3, FSWorkTime)
        ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
