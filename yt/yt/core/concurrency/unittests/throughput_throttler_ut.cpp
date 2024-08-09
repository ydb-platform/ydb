#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/profiling/timing.h>

#include <random>
#include <thread>
#include <vector>

namespace NYT::NConcurrency {
namespace {

using namespace NLogging;

using namespace testing;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

TEST(TReconfigurableThroughputThrottlerTest, NoLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>());

    NProfiling::TWallTimer timer;
    for (int i = 0; i < 1000; ++i) {
        throttler->Throttle(1).Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100u);
}

TEST(TReconfigurableThroughputThrottlerTest, CannotBeAbusedViaReconfigure)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(0));

    auto future1 = throttler->Throttle(1);
    auto future2 = throttler->Throttle(1);
    throttler->SetLimit(0);

    EXPECT_FALSE(future1.IsSet());
    EXPECT_FALSE(future2.IsSet());
}

TEST(TReconfigurableThroughputThrottlerTest, Limit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 50u);

    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 1000u);
    EXPECT_LE(duration, 3000u);
}

TEST(TReconfigurableThroughputThrottlerTest, NoOverflow)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(100_TB));

    auto* testableThrottler = static_cast<ITestableReconfigurableThroughputThrottler*>(throttler.Get());

    NProfiling::TWallTimer timer;
    testableThrottler->Throttle(1).Get().ThrowOnError();
    testableThrottler->SetLastUpdated(TInstant::Now() - TDuration::Days(1));

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 2; ++i) {
        futures.push_back(testableThrottler->Throttle(1));
        testableThrottler->SetLimit(5_TB);
    }

    WaitFor(AllSucceeded(futures)
        .WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
}

TEST(TReconfigurableThroughputThrottlerTest, FractionalPeriod)
{
    auto config = NYT::New<NYT::NConcurrency::TThroughputThrottlerConfig>();
        config->Limit = 15;
        config->Period = TDuration::Seconds(1) / 15;

    auto throttler = CreateReconfigurableThroughputThrottler(config);

    for (int i = 0; i < 10; ++i) {
        WaitFor(throttler->Throttle(1)
            .WithTimeout(TDuration::Seconds(5)))
            .ThrowOnError();
    }
}

TEST(TReconfigurableThroughputThrottlerTest, ScheduleUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(3).Get().ThrowOnError();

    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 3000u);
    EXPECT_LE(duration, 6000u);
}

TEST(TReconfigurableThroughputThrottlerTest, Update)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(1).Get().ThrowOnError();
    Sleep(TDuration::Seconds(1));
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 2000u);
}

TEST(TReconfigurableThroughputThrottlerTest, Cancel)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(5).Get().ThrowOnError();
    auto future = throttler->Throttle(1);
    future.Cancel(TError("Error"));
    auto result = future.Get();

    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(result.GetCode() == NYT::EErrorCode::Canceled);
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100u);
}

TEST(TReconfigurableThroughputThrottlerTest, ReconfigureSchedulesUpdatesProperly)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(100));
    }

    throttler->Reconfigure(TThroughputThrottlerConfig::Create(100));

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 5000u);
}

TEST(TReconfigurableThroughputThrottlerTest, SetLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(100));
    }

    throttler->SetLimit(100);

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 5000u);
}

TEST(TReconfigurableThroughputThrottlerTest, ReconfigureMustRescheduleUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(1));

    throttler->Acquire(1); // drains throttler to zero

    NProfiling::TWallTimer timer;

    auto scheduled1 = throttler->Throttle(100); // sits in front of the queue
    auto scheduled2 = throttler->Throttle(100); // also waits in the queue

    throttler->Reconfigure(TThroughputThrottlerConfig::Create(100));

    EXPECT_FALSE(scheduled2.IsSet()); // must remain waiting in the queue after Reconfigure

    scheduled2.Get().ThrowOnError();
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000u); // Reconfigure must have rescheduled the update
}

TEST(TReconfigurableThroughputThrottlerTest, Overdraft)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(100));

    throttler->Throttle(150).Get().ThrowOnError();

    EXPECT_TRUE(throttler->IsOverdraft());
    Sleep(TDuration::Seconds(2));
    EXPECT_FALSE(throttler->IsOverdraft());
}

TEST(TReconfigurableThroughputThrottlerTest, OverdraftSignificantly)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(100));

    const auto N = 3;
    NProfiling::TWallTimer timer;
    for (int i = 0; i < N; ++i) {
        throttler->Throttle(300).Get().ThrowOnError();
    }

    auto expectedElapsed = (300 * (N - 1) - 100) * 1000 / 100;
    //                             ^^1^^  ^^2^^
    // NB(coteeq):
    // 1. The last throttle overdrafts throttler and does not wait, hence minus one.
    // 2. The first throttle takes 100 of its 300 from initial throttler's amount.
    auto elapsed = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(elapsed, expectedElapsed - 100u);
    EXPECT_LE(elapsed, expectedElapsed + 100u);
}

#if !defined(_asan_enabled_) && !defined(_msan_enabled_) && !defined(_tsan_enabled_)

TEST(TReconfigurableThroughputThrottlerTest, Stress)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(100));

    const int N = 30;
    const int M = 10;

    NProfiling::TWallTimer timer;

    std::vector<std::thread> threads;
    for (int i = 0; i < N; i++) {
        threads.emplace_back([&] {
            for (int j = 0; j < M; ++j) {
                throttler->Throttle(1).Get().ThrowOnError();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000u);
}

#endif

TEST(TReconfigurableThroughputThrottlerTest, FractionalLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(0.5));

    NProfiling::TWallTimer timer;
    for (int i = 0; i < 2; ++i) {
        throttler->Throttle(1).Get().ThrowOnError();
    }
    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 1500u);
    EXPECT_LE(duration, 4000u);
}

TEST(TReconfigurableThroughputThrottlerTest, ZeroLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(0));

    throttler->SetLimit(100);

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(10));
    }

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 1000u);
}

TEST(TReconfigurableThroughputThrottlerTest, ZeroLimitDoesNotLetAnythingThrough)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(0));

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(1));
    }

    Sleep(TDuration::Seconds(2));

    // You shall not pass!
    for (const auto& future : scheduled) {
        EXPECT_FALSE(future.IsSet());
    }

    throttler->SetLimit(std::nullopt);
    // Now we should pass through in a breeze.

    NProfiling::TWallTimer timer;

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100u);
}

TEST(TReconfigurableThroughputThrottlerTest, Release)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(100));

    auto future = throttler->Throttle(100);
    EXPECT_EQ(future, VoidFuture);

    throttler->Release(100);
    future = throttler->Throttle(100);
    EXPECT_EQ(future, VoidFuture);

    future = throttler->Throttle(100);
    EXPECT_FALSE(future.IsSet());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMockThrottler)

class TMockThrottler
    : public IThroughputThrottler
{
public:
    MOCK_METHOD(TFuture<void>, Throttle, (i64 /*amount*/), (override));

    bool TryAcquire(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    i64 TryAcquireAvailable(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Acquire(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Release(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    bool IsOverdraft() override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetQueueTotalAmount() const override
    {
        YT_UNIMPLEMENTED();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetAvailable() const override
    {
        YT_UNIMPLEMENTED();
    }
};

DEFINE_REFCOUNTED_TYPE(TMockThrottler)

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingThrottlerExponentialGrowthTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Config_ = New<TPrefetchingThrottlerConfig>();
        Config_->TargetRps = 1.0;
        Config_->MinPrefetchAmount = 1;
        Config_->MaxPrefetchAmount = 1 << 30;
        Config_->Window = TDuration::MilliSeconds(100);

        Throttler_ = CreatePrefetchingThrottler(Config_, Underlying_, Logger());
    }

protected:
    TPrefetchingThrottlerConfigPtr Config_;
    TMockThrottlerPtr Underlying_ = New<TMockThrottler>();
    IThroughputThrottlerPtr Throttler_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, OneRequest)
{
    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(1)
        .WillRepeatedly(Return(VoidFuture));

    EXPECT_TRUE(Throttler_->Throttle(1).Get().IsOK());
}

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, ManyRequests)
{
    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(4)
        .WillRepeatedly(Return(VoidFuture));

    for (int i = 0; i < 1'000; ++i) {
        EXPECT_TRUE(Throttler_->Throttle(1).Get().IsOK());
    }
}

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, SpikeAmount)
{
    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(4)
        .WillRepeatedly(Return(VoidFuture));

    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(Throttler_->Throttle(1).Get().IsOK());
    }

    EXPECT_TRUE(Throttler_->Throttle(10'000'000).Get().IsOK());

    for (int i = 3; i < 1'000; ++i) {
        EXPECT_TRUE(Throttler_->Throttle(1).Get().IsOK());
    }
}

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, DoNotOverloadUnderlyingWhenTheQuotaIsExceeded)
{
    std::vector<TPromise<void>> requests;
    std::vector<TFuture<void>> replies;
    TPromise<void> lastRequest;

    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(AtMost(9))
        .WillRepeatedly(DoAll(
            [&] { lastRequest = requests.emplace_back(NewPromise<void>()); },
            ReturnPointee(&lastRequest)));

    for (int i = 0; i < 100; ++i) {
        replies.emplace_back(Throttler_->Throttle(1));
    }

    for (auto& request : requests) {
        request.Set();
    }
}

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, DoNotHangUpAfterAnError)
{
    std::vector<TPromise<void>> requests;
    TPromise<void> lastRequest;

    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(AtLeast(2))
        .WillRepeatedly(DoAll(
            [&] { lastRequest = requests.emplace_back(NewPromise<void>()); },
            ReturnPointee(&lastRequest)));

    auto failedRequest = Throttler_->Throttle(10);
    requests[0].Set(TError(NYT::EErrorCode::Generic, "Test error"));
    EXPECT_FALSE(failedRequest.Get().IsOK());

    YT_UNUSED_FUTURE(Throttler_->Throttle(1));
}

TEST_F(TPrefetchingThrottlerExponentialGrowthTest, Release)
{
    EXPECT_CALL(*Underlying_, Throttle(_))
        .Times(1)
        .WillRepeatedly(Return(VoidFuture));

    EXPECT_TRUE(Throttler_->Throttle(1).Get().IsOK());
    EXPECT_TRUE(Throttler_->IsOverdraft());

    Throttler_->Release(1);
    EXPECT_FALSE(Throttler_->IsOverdraft());

    EXPECT_EQ(Throttler_->Throttle(1), VoidFuture);
    EXPECT_TRUE(Throttler_->IsOverdraft());
}

////////////////////////////////////////////////////////////////////////////////

struct TStressParameters
{
    TDuration TestDuration;
    TDuration IterationDuration = TDuration::MilliSeconds(100);
    int RequestsPerIteration = 10'000;
    int IterationsPerStep = 0;
    int PassCount = 1;
    double StepMultiplier = 1.0;
    double MaxUnderlyingAmountMultiplier = 2.0;
    double AllowedRpsOverflowMultiplier;
    double TryAcquireAllProbability = -1.0;
    double ErrorProbability = -1.0;
};

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingStressTest
    : public ::testing::TestWithParam<TStressParameters>
{
public:
    void SetUp() override
    {
        Config_ = New<TPrefetchingThrottlerConfig>();
        Config_->TargetRps = 10.0;
        Config_->MinPrefetchAmount = 1;
        Config_->MaxPrefetchAmount = 1 << 30;
        Config_->Window = TDuration::Seconds(1);

        Throttler_ = CreatePrefetchingThrottler(Config_, Underlying_, Logger());
    }

protected:
    TPrefetchingThrottlerConfigPtr Config_;
    TMockThrottlerPtr Underlying_ = New<TMockThrottler>();
    IThroughputThrottlerPtr Throttler_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TPrefetchingStressTest, Stress)
{
    auto parameters = GetParam();

    std::mt19937 engine(314159265);
    std::uniform_int_distribution<int> underlyingResponses(0, 2);
    std::uniform_int_distribution<int> incomingAmount(1, 1 << 15);
    std::uniform_real_distribution<double> incomingSpikeLbAmount(15.0, 20.0);
    std::uniform_real_distribution<double> probabilisticOutcome(0.0, 1.0);

    double tryAcquireProbability = 0.000'1;
    double acquireProbability = 0.000'1;

    for (int pass = 0; pass < parameters.PassCount; ++pass) {
        i64 lastUnderlyingAmount = 0;
        i64 iterationUnderlyingAmount = 0;
        int underlyingRequestCount = 0;

        std::deque<TPromise<void>> requests;
        std::vector<TFuture<void>> replies;
        TPromise<void> lastRequest;
        int processedUnderlyingRequests = 0;

        EXPECT_CALL(*Underlying_, Throttle(_))
            .WillRepeatedly(DoAll(
                SaveArg<0>(&lastUnderlyingAmount),
                [&] {
                    lastRequest = requests.emplace_back(NewPromise<void>());
                    ++underlyingRequestCount;
                    iterationUnderlyingAmount += lastUnderlyingAmount;
                },
                ReturnPointee(&lastRequest)));

        auto processUnderlyingRequest = [&] (double errorProbability) {
            if (!requests.empty()) {
                if (probabilisticOutcome(engine) < errorProbability) {
                    requests.front().Set(TError(NYT::EErrorCode::Generic, "Test error"));
                } else {
                    requests.front().Set();
                }
                requests.pop_front();
                ++processedUnderlyingRequests;
            }
        };

        int iteration = 0;
        auto requestsPerIteration = parameters.RequestsPerIteration;

        auto start = TInstant::Now();
        TInstant iterationStart;

        while ((iterationStart = TInstant::Now()) < start + parameters.TestDuration / parameters.PassCount) {
            iterationUnderlyingAmount = 0;
            underlyingRequestCount = 0;
            i64 iterationIncomingAmount = 0;

            auto spikeProbability = 1.0 / requestsPerIteration;

            for (int i = 0; i < requestsPerIteration; ++i) {
                if (probabilisticOutcome(engine) < parameters.TryAcquireAllProbability) {
                    Throttler_->TryAcquireAvailable(std::numeric_limits<i64>::max());
                }

                i64 amount = 0;
                if (probabilisticOutcome(engine) < spikeProbability) {
                    amount = std::pow(2.0, incomingSpikeLbAmount(engine));
                } else {
                    amount = incomingAmount(engine);
                }
                iterationIncomingAmount += amount;
                auto outcome = probabilisticOutcome(engine);
                if (outcome < acquireProbability) {
                    Throttler_->Acquire(amount);
                } else if (outcome < acquireProbability + tryAcquireProbability) {
                    Throttler_->TryAcquire(amount);
                } else {
                    replies.emplace_back(Throttler_->Throttle(amount));
                }

                auto requestFinish = TInstant::Now();
                auto realDuration = requestFinish - iterationStart;
                if (realDuration < i * parameters.IterationDuration / requestsPerIteration) {
                    Sleep(i * parameters.IterationDuration / requestsPerIteration - realDuration);
                }
            }

            auto iterationFinish = TInstant::Now();
            auto realDuration = iterationFinish - iterationStart;
            if (realDuration < parameters.IterationDuration) {
                Sleep(parameters.IterationDuration - realDuration);
            }

            int underlyingResponseCount = underlyingResponses(engine);
            for (int i = underlyingResponseCount; i > 0; --i) {
                processUnderlyingRequest(parameters.ErrorProbability);
            }

            ++iteration;
            if (parameters.IterationsPerStep > 0 && iteration % parameters.IterationsPerStep == 0) {
                if (iterationIncomingAmount != 0) {
                    EXPECT_LE(iterationUnderlyingAmount / iterationIncomingAmount, parameters.MaxUnderlyingAmountMultiplier);
                }

                requestsPerIteration *= parameters.StepMultiplier;
            }
        }

        while (!requests.empty()) {
            processUnderlyingRequest(-1.0);
        }

        auto finish = TInstant::Now();
        auto totalDuration = finish - start;
        auto averageUnderlyingRps = processedUnderlyingRequests / totalDuration.SecondsFloat();

        EXPECT_LE(averageUnderlyingRps / Config_->TargetRps, parameters.AllowedRpsOverflowMultiplier);

        for (auto& reply : replies) {
            if (parameters.ErrorProbability > 0.0) {
                reply.Get();
            } else {
                EXPECT_TRUE(reply.Get().IsOK());
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Stress,
    TPrefetchingStressTest,
    testing::Values(
        TStressParameters {
            .TestDuration = TDuration::Seconds(50),
            .IterationsPerStep = 100,
            .StepMultiplier = 0.1,
            .AllowedRpsOverflowMultiplier = 3.0,
        },
        TStressParameters {
            .TestDuration = TDuration::Seconds(10),
            .AllowedRpsOverflowMultiplier = 5.0,
            .TryAcquireAllProbability = 0.000'1,
        },
        TStressParameters{
            .TestDuration = TDuration::Seconds(1),
            .AllowedRpsOverflowMultiplier = 20.0,
            .TryAcquireAllProbability = 0.000'1,
        },
        TStressParameters {
            .TestDuration = TDuration::Seconds(100),
            .PassCount = 10,
            .AllowedRpsOverflowMultiplier = 5.0,
            .TryAcquireAllProbability = 0.000'1,
            .ErrorProbability = 0.01,
        },
        TStressParameters{
            .TestDuration = TDuration::Seconds(10),
            .PassCount = 10,
            .AllowedRpsOverflowMultiplier = 20.0,
            .TryAcquireAllProbability = 0.000'1,
            .ErrorProbability = 0.01,
        }));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
