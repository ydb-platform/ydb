#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_looper.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/threading/event_count.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO(arkady-e1ppa): Add ManualInvoker which only runs callbacks when
// manually requested. Add test when Stop/Restart occurs during the
// intermission between Async and Sync steps.

TEST(TAsyncLooperTest, JustWorks)
{
    auto queue = New<TActionQueue>();

    auto asyncStart = BIND([invoker = queue->GetInvoker()] (bool) {
        VERIFY_INVOKER_AFFINITY(invoker);
        return BIND([] {}).AsyncVia(invoker).Run();
    });

    auto progress = std::make_shared<std::atomic<int>>(0);
    auto syncFinish = BIND([progress, invoker = queue->GetInvoker()] (bool) {
        VERIFY_INVOKER_AFFINITY(invoker);
        progress->fetch_add(1);
    });

    auto currentProgress = progress->load();

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    Sleep(TDuration::Seconds(1));
    EXPECT_EQ(currentProgress, progress->load());

    looper->Start();

    while (currentProgress == progress->load()) {
        Sleep(TDuration::MilliSeconds(1));
    }

    currentProgress = progress->load();

    while (currentProgress == progress->load()) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, Restart)
{
    auto queue = New<TActionQueue>();

    auto asyncStart = BIND([invoker = queue->GetInvoker()] (bool) {
        return BIND([] {}).AsyncVia(invoker).Run();
    });

    auto cleanStarts = std::make_shared<std::atomic<int>>(0);
    auto syncFinish = BIND([cleanStarts] (bool cleanStart) {
        if (cleanStart) {
            cleanStarts->fetch_add(1);
        }
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    while (cleanStarts->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(cleanStarts->load(), 1);

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(cleanStarts->load(), 1);

    looper->Stop();

    looper->Start();

    while (cleanStarts->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(cleanStarts->load(), 2);

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, CancelAsyncStep)
{
    auto queue = New<TActionQueue>();

    NThreading::TEvent started;
    auto promise = NewPromise<void>();

    auto asyncStart = BIND([invoker = queue->GetInvoker(), promise, &started] (bool) {
        return BIND([promise, &started] {
            started.NotifyAll();
            WaitFor(promise.ToFuture())
                .ThrowOnError();
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([] (bool) {
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    queue->Shutdown();

    EXPECT_TRUE(promise.IsCanceled());
}

TEST(TAsyncLooperTest, CancelSyncStep)
{
    auto queue = New<TActionQueue>();

    NThreading::TEvent started;
    auto promise = NewPromise<void>();

    auto asyncStart = BIND([invoker = queue->GetInvoker()] (bool) {
        return BIND([] {
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([promise, &started] (bool) {
        started.NotifyAll();
        WaitFor(promise.ToFuture())
            .ThrowOnError();
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    EXPECT_TRUE(promise.IsCanceled());

    queue->Shutdown();
}

TEST(TAsyncLooperTest, StopDuringAsyncStep)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;
    NThreading::TEvent started;

    auto asyncStart = BIND([invoker = queue->GetInvoker(), &releaseAsync, &started] (bool) {
        return BIND([&releaseAsync, &started] {
            started.NotifyAll();
            releaseAsync.Wait();
        }).AsyncVia(invoker).Run();
    });

    auto mustBeFalse = std::make_shared<std::atomic<bool>>(false);
    auto syncFinish = BIND([mustBeFalse] (bool) {
        mustBeFalse->store(true);
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    releaseAsync.NotifyAll();

    // We cannot ensure that callback will be submitted
    // So we just wait a little bit.
    Sleep(TDuration::Seconds(1));

    // Ensure queue is empty
    queue->Shutdown(/*graceful*/ true);

    EXPECT_FALSE(mustBeFalse->load());
}

TEST(TAsyncLooperTest, StopDuringAsyncStepWaitFor)
{
    auto queue = New<TActionQueue>();

    auto releaseAsync = NewPromise<void>();
    NThreading::TEvent started;

    auto asyncStart = BIND([invoker = queue->GetInvoker(), &started, releaseAsync] (bool) {
        return BIND([releaseAsync, &started] {
            started.NotifyAll();
            WaitFor(releaseAsync.ToFuture())
                .ThrowOnError();
        }).AsyncVia(invoker).Run();
    });

    auto mustBeFalse = std::make_shared<std::atomic<bool>>(false);
    auto syncFinish = BIND([mustBeFalse] (bool) {
        mustBeFalse->store(true);
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    releaseAsync.Set();

    // We cannot ensure that callback will be submitted
    // So we just wait a little bit.
    Sleep(TDuration::Seconds(1));

    // Ensure queue is empty
    queue->Shutdown(/*graceful*/ true);

    EXPECT_FALSE(mustBeFalse->load());
}

TEST(TAsyncLooperTest, RestartDuringAsyncStep)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;

    auto asyncRunCount = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker(), &releaseAsync, asyncRunCount] (bool) {
        return BIND([&releaseAsync, asyncRunCount] {
            asyncRunCount->fetch_add(1);
            releaseAsync.Wait();
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([] (bool) {
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    while (asyncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(asyncRunCount->load(), 1);

    looper->Stop();
    looper->Start();

    releaseAsync.NotifyAll();

    while (asyncRunCount->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, RestartDuringAsyncStepWaitFor)
{
    auto queue = New<TActionQueue>();

    auto releaseAsync = NewPromise<void>();

    auto asyncRunCount = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker(), releaseAsync, asyncRunCount] (bool) {
        return BIND([releaseAsync, asyncRunCount] {
            asyncRunCount->fetch_add(1);
            WaitFor(releaseAsync.ToFuture())
                .ThrowOnError();
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([] (bool) {
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    while (asyncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(asyncRunCount->load(), 1);

    looper->Stop();
    looper->Start();

    releaseAsync.Set();

    while (asyncRunCount->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, StopDuringAsyncStepPreparation)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;
    NThreading::TEvent started;

    auto mustBeFalse = std::make_shared<std::atomic<bool>>(false);
    auto asyncStart = BIND([invoker = queue->GetInvoker(), &releaseAsync, &started, mustBeFalse] (bool) {
        started.NotifyAll();
        releaseAsync.Wait();

        // NB(arkady-e1ppa): Callback below will be submitted to the same action queue
        // current callback is running on. Thus we guarantee that it will not
        // be finished before looper internals get to cancel it thus
        // preventing the loop from occuring.
        return BIND([mustBeFalse] {
            mustBeFalse->store(true);
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([] (bool) {
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    queue->GetInvoker()->Invoke(BIND([looper] {
        looper->Start();
    }));

    started.Wait();

    looper->Stop();

    releaseAsync.NotifyAll();

    // We cannot ensure that callback will be submitted
    // So we just wait a little bit.
    Sleep(TDuration::Seconds(1));

    // Ensure queue is empty
    queue->Shutdown(/*graceful*/ true);

    EXPECT_FALSE(mustBeFalse->load());
}

TEST(TAsyncLooperTest, RestartDuringAsyncStepPreparation1)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;
    NThreading::TEvent started;

    auto asyncRunCount = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker(), &releaseAsync, &started, asyncRunCount] (bool) {
        started.NotifyAll();
        releaseAsync.Wait();
        return BIND([asyncRunCount] {
            asyncRunCount->fetch_add(1);
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([] (bool) {
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();
    looper->Start();

    releaseAsync.NotifyAll();

    while (asyncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, RestartDuringAsyncStepPreparation2)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;
    NThreading::TEvent secondIterationStarted;

    auto asyncCleanStarts = std::make_shared<std::atomic<int>>(0);
    auto syncCleanStarts = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker(), &releaseAsync, &secondIterationStarted, asyncCleanStarts, syncCleanStarts] (bool cleanStart) {
        if (cleanStart) {
            asyncCleanStarts->fetch_add(1);
        }

        if (syncCleanStarts->load() == 1) {
            // Clean start has fully finished.
            secondIterationStarted.NotifyAll();
            releaseAsync.Wait();
        }

        return BIND([] {
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([syncCleanStarts] (bool cleanStart) {
        if (cleanStart) {
            syncCleanStarts->fetch_add(1);
        }
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    secondIterationStarted.Wait();

    EXPECT_EQ(asyncCleanStarts->load(), 1);
    EXPECT_EQ(syncCleanStarts->load(), 1);

    looper->Stop();
    looper->Start();

    releaseAsync.NotifyAll();

    while (syncCleanStarts->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(asyncCleanStarts->load(), 2);
    EXPECT_EQ(syncCleanStarts->load(), 2);

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, StopDuringSyncStep)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;
    NThreading::TEvent started;

    auto asyncRunCount = std::make_shared<std::atomic<int>>(0);
    auto asyncStart = BIND([invoker = queue->GetInvoker(), asyncRunCount] (bool) {
        return BIND([asyncRunCount] {
            asyncRunCount->fetch_add(1);
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([&releaseAsync, &started] (bool) {
        started.NotifyAll();
        releaseAsync.Wait();
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    releaseAsync.NotifyAll();

    // We cannot ensure that callback will be submitted
    // So we just wait a little bit.
    Sleep(TDuration::Seconds(1));

    // Ensure queue is empty
    queue->Shutdown(/*graceful*/ true);

    EXPECT_EQ(asyncRunCount->load(), 1);
}

TEST(TAsyncLooperTest, StopDuringSyncStepWaitFor)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    auto releaseAsync = NewPromise<void>();
    NThreading::TEvent started;

    auto asyncRunCount = std::make_shared<std::atomic<int>>(0);
    auto asyncStart = BIND([invoker = queue->GetInvoker(), asyncRunCount] (bool) {
        return BIND([asyncRunCount] {
            asyncRunCount->fetch_add(1);
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([releaseAsync, &started] (bool) {
        started.NotifyAll();
        WaitFor(releaseAsync.ToFuture())
            .ThrowOnError();
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    started.Wait();

    looper->Stop();

    releaseAsync.Set();

    // We cannot ensure that callback will be submitted
    // So we just wait a little bit.
    Sleep(TDuration::Seconds(1));

    // Ensure queue is empty
    queue->Shutdown(/*graceful*/ true);

    EXPECT_EQ(asyncRunCount->load(), 1);
}

TEST(TAsyncLooperTest, RestartDuringSyncStep)
{
    auto queue = New<TActionQueue>();

    // We use event and not future to
    // ignore cancelation in this test.
    NThreading::TEvent releaseAsync;

    auto syncRunCount = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker()] (bool) {
        return BIND([] {
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([&releaseAsync, syncRunCount] (bool) {
        syncRunCount->fetch_add(1);
        releaseAsync.Wait();
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    while (syncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(syncRunCount->load(), 1);

    looper->Stop();
    looper->Start();

    releaseAsync.NotifyAll();

    while (syncRunCount->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, RestartDuringSyncStepWaitFor)
{
    auto queue = New<TActionQueue>();

    auto releaseAsync = NewPromise<void>();

    auto syncRunCount = std::make_shared<std::atomic<int>>(0);

    auto asyncStart = BIND([invoker = queue->GetInvoker()] (bool) {
        return BIND([] {
        }).AsyncVia(invoker).Run();
    });

    auto syncFinish = BIND([releaseAsync, syncRunCount] (bool) {
        syncRunCount->fetch_add(1);
        WaitFor(releaseAsync.ToFuture())
            .ThrowOnError();
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    while (syncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    EXPECT_EQ(syncRunCount->load(), 1);

    looper->Stop();
    looper->Start();

    releaseAsync.Set();

    while (syncRunCount->load() == 1) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

TEST(TAsyncLooperTest, NullFuture)
{
    auto queue = New<TActionQueue>();

    auto switcher = std::make_shared<std::atomic<bool>>(false);
    NThreading::TEvent loopBroken;

    auto asyncStart = BIND([invoker = queue->GetInvoker(), switcher, &loopBroken] (bool) {
        if (!switcher->load()) {
            loopBroken.NotifyAll();
            return TFuture<void>();
        }

        return BIND([] {}).AsyncVia(invoker).Run();
    });

    auto syncRunCount = std::make_shared<std::atomic<int>>(0);
    auto syncFinish = BIND([syncRunCount] (bool) {
        syncRunCount->fetch_add(1);
    });

    auto looper = New<TAsyncLooper>(
        queue->GetInvoker(),
        asyncStart,
        syncFinish);

    looper->Start();

    loopBroken.Wait();

    EXPECT_EQ(syncRunCount->load(), 0);

    switcher->store(true);
    looper->Stop();
    looper->Start();

    while (syncRunCount->load() == 0) {
        Sleep(TDuration::MilliSeconds(1));
    }

    looper->Stop();
    queue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT:::NConcurrency
