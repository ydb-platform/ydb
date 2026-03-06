#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/pollable_detail.h>
#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <util/system/env.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ExpectSuccessfullySetFuture(const TFuture<T>& future)
{
    YT_VERIFY(WaitForFast(future.WithTimeout(TDuration::Seconds(15))).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TPollableMock
    : public IPollable
{
public:
    explicit TPollableMock(std::string loggingTag = {})
        : LoggingTag_(std::move(loggingTag))
    { }

    void SetCookie(IPollable::TCookiePtr cookie) override
    {
        Cookie_ = std::move(cookie);
    }

    void* GetCookie() const override
    {
        return static_cast<void*>(Cookie_.Get());
    }

    const std::string& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    void OnEvent(EPollControl control) override
    {
        // NB: Retry is the only event we trigger in this unittest via |IPoller::Retry|.
        YT_VERIFY(control == EPollControl::Retry);
        YT_VERIFY(!ShutdownPromise_.IsSet());
        RetryPromise_.Set();
    }

    void OnShutdown() override
    {
        ShutdownPromise_.Set();
    }

    TFuture<void> GetRetryFuture() const
    {
        return RetryPromise_;
    }

    TFuture<void> GetShutdownFuture() const
    {
        return ShutdownPromise_;
    }

private:
    const std::string LoggingTag_;

    const TPromise<void> RetryPromise_ = NewPromise<void>();
    const TPromise<void> ShutdownPromise_ = NewPromise<void>();

    IPollable::TCookiePtr Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolPollerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Poller = CreateThreadPoolPoller(InitialThreadCount, "TestPoller");
    }

    void TearDown() override
    {
        Poller->Shutdown();
    }

protected:
    const int InitialThreadCount = 4;

    IThreadPoolPollerPtr Poller;
};

TEST_F(TThreadPoolPollerTest, SimplePollable)
{
    auto pollable = New<TPollableMock>();
    EXPECT_TRUE(Poller->TryRegister(pollable));
    Poller->Retry(pollable);

    ExpectSuccessfullySetFuture(pollable->GetRetryFuture());

    std::vector<TFuture<void>> futures{
        Poller->Unregister(pollable),
        pollable->GetShutdownFuture()
    };
    ExpectSuccessfullySetFuture(AllSucceeded(futures));
}

TEST_F(TThreadPoolPollerTest, SimpleCallback)
{
    auto promise = NewPromise<void>();
    auto callback = BIND([=] { promise.Set(); });

    Poller->GetInvoker()->Invoke(callback);

    ExpectSuccessfullySetFuture(promise.ToFuture());
}

TEST_F(TThreadPoolPollerTest, SimpleReconfigure)
{
    auto pollable = New<TPollableMock>();
    EXPECT_TRUE(Poller->TryRegister(pollable));

    Poller->SetThreadCount(InitialThreadCount * 2);

    std::vector<TFuture<void>> futures{
        Poller->Unregister(pollable),
        pollable->GetShutdownFuture()
    };
    ExpectSuccessfullySetFuture(AllSucceeded(futures));
}

TEST_F(TThreadPoolPollerTest, Stress)
{
    std::vector<std::thread> threads;

    auto envIterCount = GetEnv("ITER_COUNT");
#if defined(__aarch64__) || defined(__arm64__)
    constexpr int defaultIterCount = 10'000;
#else
    constexpr int defaultIterCount = 20'000;
#endif
    int iterCount = envIterCount.empty() ? defaultIterCount : FromString<int>(envIterCount);

    std::vector<std::thread> auxThreads;
    auxThreads.emplace_back([&] {
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&, i] {
                std::vector<TIntrusivePtr<TPollableMock>> pollables;
                for (int j = 0; j < iterCount; ++j) {
                    pollables.push_back(New<TPollableMock>(Format("%v/%05d", i, j)));
                    EXPECT_TRUE(Poller->TryRegister(pollables.back()));
                }

                std::this_thread::yield();

                std::vector<TFuture<void>> retryFutures;
                std::vector<TFuture<void>> unregisterFutures;
                for (int j = 0; j < iterCount; j += 2) {
                    Poller->Retry(pollables[j]);
                    retryFutures.push_back(pollables[j]->GetRetryFuture());

                    Poller->Retry(pollables[j + 1]);
                    std::vector<TFuture<void>> futures{
                        Poller->Unregister(pollables[j + 1]),
                        pollables[j + 1]->GetShutdownFuture()
                    };
                    unregisterFutures.push_back(AllSucceeded(futures));
                }

                ExpectSuccessfullySetFuture(AllSucceeded(retryFutures));
                ExpectSuccessfullySetFuture(AllSucceeded(unregisterFutures));
            });

            std::this_thread::yield();
        }
    });
    auxThreads.emplace_back([&] {
        for (int j = 0; j < 10; ++j) {
            for (int i = 1, sign = -1; i < 10; ++i, sign *= -1) {
                Poller->SetThreadCount(InitialThreadCount + sign * i);
            }
            std::this_thread::yield();
        }
    });

    for (auto& thread : auxThreads) {
        thread.join();
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

#ifdef _unix_

TEST_F(TThreadPoolPollerTest, ForgotToUnarm)
{
    int pipes[2];
    EXPECT_EQ(::pipe(pipes), 0);

    {
        IPollablePtr pollable = MakeSimplePollable([] (IPollable&, EPollControl) {}, "simple");
        EXPECT_TRUE(Poller->TryRegister(pollable));
        Poller->Arm(pipes[0], pollable, EPollControl::Read);
        WaitForFast(Poller->Unregister(pollable))
            .ThrowOnError();
    }

    Sleep(TDuration::MilliSeconds(100));
    EXPECT_EQ(1, ::write(pipes[1], "a", 1));
    Sleep(TDuration::MilliSeconds(100));

    ::close(pipes[0]);
    ::close(pipes[1]);
}

TEST_F(TThreadPoolPollerTest, SelfRearmStress)
{
    int pipes[2];
    EXPECT_EQ(::pipe(pipes), 0);

    EXPECT_EQ(1, ::write(pipes[1], "a", 1));

    for (int i = 0; i < 1000; ++i) {
        IPollablePtr pollable = MakeSimplePollable([&] (IPollable& self, EPollControl) {
            Poller->Arm(pipes[0], MakeStrong(&self), EPollControl::Read);
        }, "simple");
        EXPECT_TRUE(Poller->TryRegister(pollable));
        Poller->Arm(pipes[0], pollable, EPollControl::Read);

        Sleep(TDuration::MilliSeconds(10));

        WaitForFast(Poller->Unregister(pollable))
            .ThrowOnError();

        Sleep(TDuration::MilliSeconds(10));
    }

    Sleep(TDuration::MilliSeconds(100));
    ::close(pipes[0]);
    ::close(pipes[1]);
}

#endif // _unix_

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
