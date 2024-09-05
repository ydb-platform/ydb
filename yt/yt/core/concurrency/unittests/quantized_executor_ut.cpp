#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/quantized_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <limits>
#include <random>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSimpleCallbackProvider
    : public ICallbackProvider
{
public:
    explicit TSimpleCallbackProvider(i64 iterations)
        : Iterations_(iterations)
    { }

    TCallback<void()> ExtractCallback() override
    {
        if (IterationIndex_ < Iterations_) {
            ++IterationIndex_;
            return BIND([this, this_ = MakeStrong(this)] {
                Sleep(TDuration::MilliSeconds(5));
                ++Counter_;
            });
        } else {
            return {};
        }
    }

    i64 GetCounter() const
    {
        return Counter_;
    }

private:
    const i64 Iterations_;
    i64 IterationIndex_ = 0;

    std::atomic<i64> Counter_ = 0;
};

class TInitializingCallbackProvider
    : public ICallbackProvider
{
public:
    std::function<void()> GetInitializer()
    {
        return [this, this_ = MakeStrong(this)] {
            Initialized_.store(true);
        };
    }

    TCallback<void()> ExtractCallback() override
    {
        if (IsFinished()) {
            return {};
        }
        return BIND([this, this_ = MakeStrong(this)] {
            Finished_.store(true);
        });
    }

    bool IsInitialized() const
    {
        return Initialized_.load();
    }

    bool IsFinished() const
    {
        return Finished_.load();
    }

private:
    std::atomic<bool> Initialized_ = false;
    std::atomic<bool> Finished_ = false;
};

class TLongCallbackProvider
    : public ICallbackProvider
{
public:
    explicit TLongCallbackProvider(i64 iterations)
        : Iterations_(iterations)
    { }

    TCallback<void()> ExtractCallback() override
    {
        if (!Extracted_) {
            Extracted_ = true;
            return BIND([this, this_ = MakeStrong(this)] {
                for (int iteration = 0; iteration < Iterations_; ++iteration) {
                    Sleep(TDuration::MilliSeconds(10));
                    Yield();
                }

                Completed_ = true;
            });
        } else {
            return {};
        }
    }

    bool IsCompleted() const
    {
        return Completed_;
    }

private:
    const i64 Iterations_;
    bool Extracted_ = false;

    std::atomic<bool> Completed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TQuantizedExecutorTest
    : public ::testing::Test
{
protected:
    TIntrusivePtr<TSimpleCallbackProvider> SimpleCallbackProvider_;
    TIntrusivePtr<TLongCallbackProvider> LongCallbackProvider_;
    IQuantizedExecutorPtr Executor_;

    void InitSimple(int workerCount, i64 iterationCount)
    {
        SimpleCallbackProvider_ = New<TSimpleCallbackProvider>(iterationCount);
        Executor_ = CreateQuantizedExecutor("test", SimpleCallbackProvider_, {.WorkerCount = workerCount});
    }

    void InitLong(int workerCount, i64 iterationCount)
    {
        LongCallbackProvider_ = New<TLongCallbackProvider>(iterationCount);
        Executor_ = CreateQuantizedExecutor("test", LongCallbackProvider_, {.WorkerCount = workerCount});
    }
};

TEST_F(TQuantizedExecutorTest, Simple)
{
    InitSimple(/*workerCount*/ 1, /*iterationCount*/ 100);

    WaitFor(Executor_->Run(TDuration::Max()))
        .ThrowOnError();

    EXPECT_EQ(SimpleCallbackProvider_->GetCounter(), 100);
}

TEST_F(TQuantizedExecutorTest, Timeout)
{
    InitSimple(/*workerCount*/ 4, /*iterationCount*/ std::numeric_limits<i64>::max());

    for (int index = 1; index <= 10; ++index) {
        WaitFor(Executor_->Run(TDuration::MilliSeconds(100)))
            .ThrowOnError();

        auto counter = SimpleCallbackProvider_->GetCounter();
        EXPECT_LE(counter, /*workerCount*/ 4 * /*milliseconds*/ 100.0 / /*period*/ 5 * index * 1.25);
    }
}

TEST_F(TQuantizedExecutorTest, LongCallback1)
{
    InitLong(/*workerCount*/ 4, /*iterationCount*/ 20);

    auto future = Executor_->Run(TDuration::MilliSeconds(500));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(LongCallbackProvider_->IsCompleted());
}

TEST_F(TQuantizedExecutorTest, LongCallback2)
{
    InitLong(/*workerCount*/ 4, /*iterationCount*/ 100);

    for (int index = 0; index < 9; ++index) {
        WaitFor(Executor_->Run(TDuration::MilliSeconds(100)))
            .ThrowOnError();
        EXPECT_FALSE(LongCallbackProvider_->IsCompleted());
    }

    WaitFor(Executor_->Run(TDuration::MilliSeconds(1000)))
        .ThrowOnError();
    EXPECT_TRUE(LongCallbackProvider_->IsCompleted());
}

TEST_F(TQuantizedExecutorTest, Reconfigure)
{
    InitSimple(/*workerCount*/ 10, /*iterationCount*/ std::numeric_limits<i64>::max());

    i64 lastCounter = 0;

    auto run = [&] (int workerCount) {
        Executor_->Reconfigure(workerCount);

        WaitFor(Executor_->Run(TDuration::MilliSeconds(100)))
            .ThrowOnError();

        auto counter = SimpleCallbackProvider_->GetCounter();
        auto result = counter - lastCounter;
        lastCounter = counter;

        return result;
    };

    std::mt19937 rng(42);

    for (int index = 0; index < 10; ++index) {
        auto workerCount = rng() % 5  + 1;
        auto increment = run(workerCount);

        EXPECT_LE(increment, workerCount * /*milliseconds*/ 100.0 / /*period*/ 5 * 1.25);
    }
}

TEST_F(TQuantizedExecutorTest, WorkerInitializer)
{
    auto callbackProvider = New<TInitializingCallbackProvider>();
    EXPECT_FALSE(callbackProvider->IsFinished());

    Executor_ = CreateQuantizedExecutor("test", callbackProvider, {.ThreadInitializer = callbackProvider->GetInitializer()});

    EXPECT_FALSE(callbackProvider->IsFinished());

    WaitFor(Executor_->Run(TDuration::MilliSeconds(300)))
        .ThrowOnError();

    EXPECT_TRUE(callbackProvider->IsInitialized());
    EXPECT_TRUE(callbackProvider->IsFinished());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
