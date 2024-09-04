#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <util/system/env.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ExpectSuccessfullySetFuture(const TFuture<T>& future)
{
    YT_VERIFY(future.WithTimeout(TDuration::Seconds(15)).Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TPollableMock
    : public IPollable
{
public:
    explicit TPollableMock(TString loggingTag = {})
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

    const TString& GetLoggingTag() const override
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
    const TString LoggingTag_;

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

    Poller->Reconfigure(InitialThreadCount * 2);

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
                Poller->Reconfigure(InitialThreadCount + sign * i);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
