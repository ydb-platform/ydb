#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/adaptive_hedging_manager.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool AreDurationsEqual(TDuration lhs, TDuration rhs, TDuration eps = TDuration::MicroSeconds(10))
{
    return lhs > rhs
        ? (lhs - rhs) < eps
        : (rhs - lhs) < eps;
}

void ExpectStatisticsEqual(const TAdaptiveHedgingManagerStatistics& lhs, const TAdaptiveHedgingManagerStatistics& rhs)
{
    EXPECT_EQ(lhs.PrimaryRequestCount, rhs.PrimaryRequestCount);
    EXPECT_EQ(lhs.SecondaryRequestCount, rhs.SecondaryRequestCount);
    EXPECT_EQ(lhs.QueuedRequestCount, rhs.QueuedRequestCount);
    EXPECT_EQ(lhs.MaxQueueSize, rhs.MaxQueueSize);
    EXPECT_TRUE(AreDurationsEqual(lhs.HedgingDelay, rhs.HedgingDelay));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestSecondaryRequestGenerator)

class TTestSecondaryRequestGenerator final
{
public:
    void GenerateSecondaryRequest()
    {
        HedgingWaitTime_.Set(TInstant::Now() - CreationTime_);
    }

    TFuture<TDuration> GetHedgingWaitTimeFuture() const
    {
        return HedgingWaitTime_;
    }

private:
    const TInstant CreationTime_ = TInstant::Now();

    const TPromise<TDuration> HedgingWaitTime_ = NewPromise<TDuration>();
};

DEFINE_REFCOUNTED_TYPE(TTestSecondaryRequestGenerator)

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveHedgingManagerTest
    : public ::testing::Test
{
protected:
    const TAdaptiveHedgingManagerConfigPtr Config_ = New<TAdaptiveHedgingManagerConfig>();

    IAdaptiveHedgingManagerPtr HedgingManager_;

    void CreateHedgingManager()
    {
        if (!Config_->SecondaryRequestRatio) {
            Config_->SecondaryRequestRatio = 0.1;
        }
        Config_->Postprocess();
        HedgingManager_ = CreateAdaptiveHedgingManager(Config_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAdaptiveHedgingManagerTest, Simple)
{
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise = NewPromise<void>();
    auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator->GenerateSecondaryRequest();
    }));

    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator->GetHedgingWaitTimeFuture())
        .ValueOrThrow();

    EXPECT_GT(hedgingWaitTime, TDuration::MilliSeconds(100));
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    ExpectStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .SecondaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        });
}

TEST_F(TAdaptiveHedgingManagerTest, NoHedging)
{
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise = NewPromise<void>();
    auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator->GenerateSecondaryRequest();
    }));

    promise.Set();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_FALSE(secondaryRequestGenerator->GetHedgingWaitTimeFuture().IsSet());

    ExpectStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay / Config_->HedgingDelayTuneFactor,
        });
}

TEST_F(TAdaptiveHedgingManagerTest, ThrottledHedging)
{
    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise1.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow();
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise2.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    ExpectStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 2,
            .SecondaryRequestCount = 1,
            .QueuedRequestCount = 1,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        });
}

TEST_F(TAdaptiveHedgingManagerTest, DelayTuned)
{
    auto hedgingDelay = TDuration::MilliSeconds(100);

    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = hedgingDelay;
    Config_->SecondaryRequestRatio = 0.999;
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise1.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    promise1.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise2.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    promise2.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise3.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator3->GenerateSecondaryRequest();
    }));
    Y_UNUSED(WaitFor(secondaryRequestGenerator3->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    hedgingDelay *= std::pow(Config_->HedgingDelayTuneFactor, 1. / *Config_->SecondaryRequestRatio - 1);
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise4 = NewPromise<void>();
    auto secondaryRequestGenerator4 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise4.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator4->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator4->GetHedgingWaitTimeFuture().IsSet());

    hedgingDelay *= std::pow(Config_->HedgingDelayTuneFactor, 1. / *Config_->SecondaryRequestRatio - 1);
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));
}

TEST_F(TAdaptiveHedgingManagerTest, DelayConvergesToRatio)
{
    auto hedgingDelay = TDuration::MilliSeconds(50);

    Config_->MaxTokenCount = 100;
    Config_->MaxHedgingDelay = hedgingDelay;
    Config_->SecondaryRequestRatio = 0.33;
    Config_->HedgingDelayTuneFactor = 1.03;
    CreateHedgingManager();

    for (int iteration = 0; iteration < 30; ++iteration) {
        int multiplier = iteration % 3 + 1;
        for (int fastRequestIndex = 0; fastRequestIndex < 2 * multiplier; ++fastRequestIndex) {
            auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();
            HedgingManager_->RegisterRequest(OKFuture, 1, BIND([] () {
                YT_ABORT();
            }));
        }

        auto promise = NewPromise<void>();
        auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();
        HedgingManager_->RegisterRequest(promise.ToFuture(), multiplier, BIND([=] () {
            secondaryRequestGenerator->GenerateSecondaryRequest();
        }));

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
        promise.Set();

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    }

    // NB: One third of all requests shall be hedged according to the ratio parameter.
    // Check that the actual delay corresponds to the biggest delay that provides such a ratio (which is ~10ms).
    EXPECT_TRUE(AreDurationsEqual(
        HedgingManager_->CollectStatistics().HedgingDelay,
        TDuration::MilliSeconds(10),
        /*eps*/ TDuration::MilliSeconds(2)));
}

TEST_F(TAdaptiveHedgingManagerTest, IncrementToken)
{
    Config_->MaxTokenCount = 1;
    Config_->SecondaryRequestRatio = 0.5;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise1.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    Y_UNUSED(WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise2.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise3.ToFuture(), 1, BIND([=] () {
        secondaryRequestGenerator3->GenerateSecondaryRequest();
    }));

    // NB: Token from the third request let us hedge the second one.
    EXPECT_TRUE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator3->GetHedgingWaitTimeFuture().IsSet());

    ExpectStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 3,
            .SecondaryRequestCount = 2,
            .QueuedRequestCount = 2,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        });
}

TEST_F(TAdaptiveHedgingManagerTest, VariousHedgingPrices)
{
    Config_->MaxTokenCount = 4;
    Config_->SecondaryRequestRatio = 1.9;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise1.ToFuture(), 3, BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow();

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise2.ToFuture(), 3, BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    auto promise3 = NewPromise<void>();
    HedgingManager_->RegisterRequest(promise3.ToFuture(), 3, BIND([=] () {
        YT_ABORT();
    }));

    WaitFor(secondaryRequestGenerator2->GetHedgingWaitTimeFuture())
        .ValueOrThrow();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    auto promise4 = NewPromise<void>();
    auto secondaryRequestGenerator4 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise4.ToFuture(), 2, BIND([=] () {
        secondaryRequestGenerator4->GenerateSecondaryRequest();
    }));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator4->GetHedgingWaitTimeFuture().IsSet());

    promise3.Set();

    auto promise5 = NewPromise<void>();
    auto secondaryRequestGenerator5 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise5.ToFuture(), 2, BIND([=] () {
        secondaryRequestGenerator5->GenerateSecondaryRequest();
    }));

    WaitFor(secondaryRequestGenerator4->GetHedgingWaitTimeFuture())
        .ValueOrThrow();
    WaitFor(secondaryRequestGenerator5->GetHedgingWaitTimeFuture())
        .ValueOrThrow();

    ExpectStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 5,
            .SecondaryRequestCount = 4,
            .QueuedRequestCount = 3,
            .MaxQueueSize = 2,
            .HedgingDelay = Config_->MaxHedgingDelay,
        });
}

TEST_F(TAdaptiveHedgingManagerTest, Stress)
{
    Config_->MaxTokenCount = 10;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);

    CreateHedgingManager();

    struct TTestHedgedRequest
    {
        explicit TTestHedgedRequest(int index)
            : Immediate(index % 5 == 0)
            , SlowPrimaryResponse(index % 2 == 0)
        { }

        const TPromise<void> Promise = NewPromise<void>();
        const TTestSecondaryRequestGeneratorPtr Generator = New<TTestSecondaryRequestGenerator>();

        const bool Immediate;
        const bool SlowPrimaryResponse;
    };

    std::vector<TTestHedgedRequest> requests;

    for (int iter = 0; iter < 20; ++iter) {
        std::vector<int> requestIndexesWithSlowResponse;
        for (int index = 0; index < 5; ++index) {
            auto& request = requests.emplace_back(iter * 5 + index);
            if (request.Immediate) {
                request.Promise.Set();
            } else if (request.SlowPrimaryResponse) {
                requestIndexesWithSlowResponse.push_back(requests.size() - 1);
            }

            HedgingManager_->RegisterRequest(request.Promise, 1, BIND([generator = request.Generator] () {
                generator->GenerateSecondaryRequest();
            }));
        }

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        for (auto index : requestIndexesWithSlowResponse) {
            requests[index].Promise.Set();
        }
    }

    auto statistics = HedgingManager_->CollectStatistics();
    EXPECT_EQ(statistics.PrimaryRequestCount, 100);
    // 10 initial tokens and 10 more generated by 100 requests.
    EXPECT_EQ(statistics.SecondaryRequestCount, 19);
    EXPECT_LE(statistics.QueuedRequestCount, 70);
    EXPECT_GE(statistics.QueuedRequestCount, 59);
    EXPECT_GE(statistics.MaxQueueSize, 49);
}

TEST_F(TAdaptiveHedgingManagerTest, DurationCumulativeError)
{
    Config_->MaxTokenCount = 1;
    Config_->HedgingDelayTuneFactor = 1.0001;
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    CreateHedgingManager();
    int iterationCount = 50000;
    std::vector<TTestSecondaryRequestGeneratorPtr> secondaryRequestGenerators;
    for (int iteration = 0; iteration < iterationCount; ++iteration) {
        auto promise = NewPromise<void>();
        promise.Set();
        auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();
        secondaryRequestGenerators.push_back(secondaryRequestGenerator);
        HedgingManager_->RegisterRequest(promise.ToFuture(), 1, BIND([=] () {
            secondaryRequestGenerator->GenerateSecondaryRequest();
        }));
    }
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(2));
    for (const auto& generator : secondaryRequestGenerators) {
        EXPECT_FALSE(generator->GetHedgingWaitTimeFuture().IsSet());
    }
    auto statistics = HedgingManager_->CollectStatistics();
    EXPECT_GT(statistics.HedgingDelay, TDuration::MilliSeconds(5));
    EXPECT_LT(statistics.HedgingDelay, TDuration::MilliSeconds(10));
    EXPECT_TRUE(statistics.MaxQueueSize == 0);
    EXPECT_TRUE(statistics.PrimaryRequestCount == iterationCount);
    EXPECT_TRUE(statistics.SecondaryRequestCount == 0);
    EXPECT_TRUE(statistics.QueuedRequestCount == 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
