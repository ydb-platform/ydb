#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SleepQuantum = TDuration::MilliSeconds(100);

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

TEST_F(TSuspendableInvokerTest, Simple)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    suspendableInvoker->Suspend().Get();
    suspendableInvoker->Resume();
}

TEST_F(TSuspendableInvokerTest, PollSuspendFuture)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] {
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

    auto setFlagFuture = BIND([&] {
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

    BIND([&] {
        promise.Set();
    })
        .Via(suspendableInvoker)
        .Run();

    // Sleep(SleepQuantum);
    EXPECT_FALSE(promise.IsSet());
    suspendableInvoker->Resume();
    promise.ToFuture().Get();
    EXPECT_TRUE(promise.IsSet());
}

TEST_F(TSuspendableInvokerTest, ResumeBeforeFullSuspend)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] {
        Sleep(SleepQuantum);
    })
        .Via(suspendableInvoker)
        .Run();

    auto firstFuture = suspendableInvoker->Suspend();

    EXPECT_FALSE(firstFuture.IsSet());
    suspendableInvoker->Resume();
    EXPECT_FALSE(firstFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, SuspendAndImmediatelyResume)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    suspendableInvoker->Suspend().Subscribe(BIND([=] (TError /*error*/) {
        suspendableInvoker->Resume();
    }));
}

TEST_F(TSuspendableInvokerTest, AllowSuspendOnContextSwitch)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto setFlagFuture = BIND([&] {
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

TEST_F(TSuspendableInvokerTest, UnsetSuspendFutureCancelingOnResume)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    BIND([&] {
        Sleep(SleepQuantum);
    })
        .Via(suspendableInvoker)
        .Run();
    auto suspendFuture = suspendableInvoker->Suspend();
    suspendableInvoker->Resume();
    auto error = suspendFuture.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(error.GetCode() == NYT::EErrorCode::Canceled);
    EXPECT_TRUE(error.GetMessage() == "Invoker resumed before suspension completed");
    EXPECT_FALSE(suspendableInvoker->IsSuspended());
}

TEST_F(TSuspendableInvokerTest, SuspendResumeOnFinishedRace)
{
    std::atomic<bool> flag(false);
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] {
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
            .Apply(BIND([=, &flag] { flag = true; }));

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

    BIND([&] {
        Sleep(SleepQuantum);
    })
        .Via(suspendableInvoker)
        .Run();

    auto suspendFuture = suspendableInvoker->Suspend()
        .Apply(BIND([=] { suspendableInvoker->Resume(); }));

    EXPECT_TRUE(suspendFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, SuspendInApplyWhenSuspensionFailed)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] {
        Sleep(SleepQuantum);
    })
        .Via(suspendableInvoker)
        .Run();

    std::atomic<bool> flag(false);

    auto suspendFuture = suspendableInvoker->Suspend()
        .Apply(BIND([=, &flag] (const TError& error) {
            EXPECT_FALSE(error.IsOK());
            EXPECT_TRUE(error.GetCode() == NYT::EErrorCode::Canceled);
            EXPECT_TRUE(error.GetMessage() == "Invoker resumed before suspension completed");
            EXPECT_FALSE(suspendableInvoker->IsSuspended());
            flag = true;
            return suspendableInvoker->Suspend();
        }));

    suspendableInvoker->Resume();
    EXPECT_TRUE(flag);

    EXPECT_TRUE(suspendFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, VerifySerializedActionsOrder)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    suspendableInvoker->Suspend()
        .Get();

    const int totalActionCount = 100000;

    std::atomic<int> actionIndex = 0;
    std::atomic<int> reorderingCount = 0;

    for (int i = 0; i < totalActionCount / 2; ++i) {
        BIND([&actionIndex, &reorderingCount, i] {
            reorderingCount += (actionIndex != i);
            ++actionIndex;
        })
            .Via(suspendableInvoker)
            .Run();
    }

    TDelayedExecutor::Submit(
        BIND([&] {
            suspendableInvoker->Resume();
        }),
        SleepQuantum / 10);

    for (int i = totalActionCount / 2; i < totalActionCount; ++i) {
        BIND([&actionIndex, &reorderingCount, i] {
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

} // namespace
} // namespace NYT::NConcurrency

