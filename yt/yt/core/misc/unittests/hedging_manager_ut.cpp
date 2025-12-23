#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/adaptive_hedging_manager.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool AreDurationsEqual(TDuration lhs, TDuration rhs)
{
    return lhs > rhs
        ? (lhs - rhs) < TDuration::MicroSeconds(10)
        : (rhs - lhs) < TDuration::MicroSeconds(10);
}

bool AreStatisticsEqual(const TAdaptiveHedgingManagerStatistics& lhs, const TAdaptiveHedgingManagerStatistics& rhs)
{
    return
        lhs.PrimaryRequestCount == rhs.PrimaryRequestCount &&
        lhs.SecondaryRequestCount == rhs.SecondaryRequestCount &&
        lhs.QueuedRequestCount == rhs.QueuedRequestCount &&
        lhs.MaxQueueSize == rhs.MaxQueueSize &&
        AreDurationsEqual(lhs.HedgingDelay, rhs.HedgingDelay);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestSecondaryRequestGenerator)

class TTestSecondaryRequestGenerator final
{
public:
    TTestSecondaryRequestGenerator()
        : CreationTime_(TInstant::Now())
    { }

    void GenerateSecondaryRequest()
    {
        HedgingWaitTime_.Set(TInstant::Now() - CreationTime_);
    }

    TFuture<TDuration> GetHedgingWaitTimeFuture() const
    {
        return HedgingWaitTime_;
    }

private:
    const TInstant CreationTime_;

    TPromise<TDuration> HedgingWaitTime_ = NewPromise<TDuration>();
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

    HedgingManager_->RegisterRequest(promise.ToFuture(), BIND([=] () {
        secondaryRequestGenerator->GenerateSecondaryRequest();
    }));

    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator->GetHedgingWaitTimeFuture())
        .ValueOrThrow();

    EXPECT_GT(hedgingWaitTime, TDuration::MilliSeconds(100));
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    EXPECT_TRUE(AreStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .SecondaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, NoHedging)
{
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise = NewPromise<void>();
    auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise.ToFuture(), BIND([=] () {
        secondaryRequestGenerator->GenerateSecondaryRequest();
    }));

    promise.Set();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_FALSE(secondaryRequestGenerator->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay / Config_->HedgingDelayTuneFactor,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, ThrottledHedging)
{
    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise1.ToFuture(), BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow();
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();

    HedgingManager_->RegisterRequest(promise2.ToFuture(), BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 2,
            .SecondaryRequestCount = 1,
            .QueuedRequestCount = 1,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, DelayTuned)
{
    auto hedgingDelay = TDuration::MilliSeconds(100);;

    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = hedgingDelay;
    Config_->SecondaryRequestRatio = 0.999;
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise1.ToFuture(), BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    promise1.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise2.ToFuture(), BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    promise2.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));


    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise3.ToFuture(), BIND([=] () {
        secondaryRequestGenerator3->GenerateSecondaryRequest();
    }));
    Y_UNUSED(WaitFor(secondaryRequestGenerator3->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    hedgingDelay *= Config_->HedgingDelayTuneFactor;
    hedgingDelay /= *Config_->SecondaryRequestRatio;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise4 = NewPromise<void>();
    auto secondaryRequestGenerator4 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise4.ToFuture(), BIND([=] () {
        secondaryRequestGenerator4->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator4->GetHedgingWaitTimeFuture().IsSet());

    hedgingDelay = Config_->MaxHedgingDelay;
    EXPECT_TRUE(AreDurationsEqual(HedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));
}

TEST_F(TAdaptiveHedgingManagerTest, IncrementToken)
{
    Config_->MaxTokenCount = 1;
    Config_->SecondaryRequestRatio = 0.5;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);;
    CreateHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise1.ToFuture(), BIND([=] () {
        secondaryRequestGenerator1->GenerateSecondaryRequest();
    }));
    Y_UNUSED(WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise2.ToFuture(), BIND([=] () {
        secondaryRequestGenerator2->GenerateSecondaryRequest();
    }));
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    HedgingManager_->RegisterRequest(promise3.ToFuture(), BIND([=] () {
        secondaryRequestGenerator3->GenerateSecondaryRequest();
    }));

    // NB: Token from the third request let us hedge the second one.
    EXPECT_TRUE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator3->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        HedgingManager_->CollectStatistics(),
        TAdaptiveHedgingManagerStatistics{
            .PrimaryRequestCount = 3,
            .SecondaryRequestCount = 2,
            .QueuedRequestCount = 2,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
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

        TPromise<void> Promise = NewPromise<void>();
        TTestSecondaryRequestGeneratorPtr Generator = New<TTestSecondaryRequestGenerator>();

        bool Immediate;
        bool SlowPrimaryResponse;
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

            HedgingManager_->RegisterRequest(request.Promise, BIND([generator = request.Generator] () {
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
    EXPECT_GE(statistics.MaxQueueSize, 50);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
