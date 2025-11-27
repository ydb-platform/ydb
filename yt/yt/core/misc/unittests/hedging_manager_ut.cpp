#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/hedging_manager.h>
#include <yt/yt/core/misc/new_hedging_manager.h>

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

bool AreStatisticsEqual(const TNewHedgingManagerStatistics& lhs, const TNewHedgingManagerStatistics& rhs)
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

class TTestSecondaryRequestGenerator
    : public ISecondaryRequestGenerator
{
public:
    TTestSecondaryRequestGenerator()
        : CreationTime_(TInstant::Now())
    { }

    void GenerateSecondaryRequest() override
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

    IHedgingManagerPtr HedgingManager_;

    INewHedgingManagerPtr NewHedgingManager_;

    void CreateHedgingManager()
    {
        Config_->Postprocess();
        HedgingManager_ = CreateAdaptiveHedgingManager(Config_);
    }

    void CreateNewHedgingManager()
    {
        if (!Config_->SecondaryRequestRatio) {
            Config_->SecondaryRequestRatio = 0.1;
        }
        Config_->Postprocess();
        NewHedgingManager_ = CreateNewAdaptiveHedgingManager(Config_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAdaptiveHedgingManagerTest, Simple)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.1;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, RatioGreaterThanOne)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 2.0;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, RestrainHedging)
{
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.5;
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_FALSE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, ApproveHedging)
{
    Config_->MinHedgingDelay = TDuration::Seconds(1);
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.5;
    Config_->TickPeriod = TDuration::Seconds(1);
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));
}

TEST_F(TAdaptiveHedgingManagerTest, TuneHedgingDelay)
{
    Config_->MinHedgingDelay = TDuration::MilliSeconds(1);
    Config_->MaxHedgingDelay = TDuration::Seconds(1);
    Config_->MaxBackupRequestRatio = 0.1;
    Config_->HedgingDelayTuneFactor = 1e9;
    Config_->TickPeriod = TDuration::Seconds(1);
    CreateHedgingManager();

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::MilliSeconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
    EXPECT_TRUE(HedgingManager_->OnHedgingDelayPassed(1));

    Sleep(TDuration::Seconds(1));

    EXPECT_EQ(TDuration::Seconds(1), HedgingManager_->OnPrimaryRequestsStarted(1));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAdaptiveHedgingManagerTest, NewSimple)
{
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateNewHedgingManager();

    auto promise = NewPromise<void>();
    auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();

    NewHedgingManager_->RegisterRequest(promise.ToFuture(), secondaryRequestGenerator);

    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator->GetHedgingWaitTimeFuture())
        .ValueOrThrow();

    EXPECT_GT(hedgingWaitTime, TDuration::MilliSeconds(100));
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    EXPECT_TRUE(AreStatisticsEqual(
        NewHedgingManager_->CollectStatistics(),
        TNewHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .SecondaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, NewNoHedging)
{
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateNewHedgingManager();

    auto promise = NewPromise<void>();
    auto secondaryRequestGenerator = New<TTestSecondaryRequestGenerator>();

    NewHedgingManager_->RegisterRequest(promise.ToFuture(), secondaryRequestGenerator);

    promise.Set();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_FALSE(secondaryRequestGenerator->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        NewHedgingManager_->CollectStatistics(),
        TNewHedgingManagerStatistics{
            .PrimaryRequestCount = 1,
            .HedgingDelay = Config_->MaxHedgingDelay / Config_->HedgingDelayTuneFactor,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, NewThrottledHedging)
{
    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);
    CreateNewHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();

    NewHedgingManager_->RegisterRequest(promise1.ToFuture(), secondaryRequestGenerator1);
    auto hedgingWaitTime = WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow();
    EXPECT_LT(hedgingWaitTime, TDuration::MilliSeconds(200));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();

    NewHedgingManager_->RegisterRequest(promise2.ToFuture(), secondaryRequestGenerator2);
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        NewHedgingManager_->CollectStatistics(),
        TNewHedgingManagerStatistics{
            .PrimaryRequestCount = 2,
            .SecondaryRequestCount = 1,
            .QueuedRequestCount = 1,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, NewDelayTuned)
{
    auto hedgingDelay = TDuration::MilliSeconds(100);;

    Config_->MaxTokenCount = 1;
    Config_->MaxHedgingDelay = hedgingDelay;
    Config_->SecondaryRequestRatio = 0.999;
    CreateNewHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise1.ToFuture(), secondaryRequestGenerator1);
    promise1.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(NewHedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise2.ToFuture(), secondaryRequestGenerator2);
    promise2.Set();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    hedgingDelay /= Config_->HedgingDelayTuneFactor;
    EXPECT_TRUE(AreDurationsEqual(NewHedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));


    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise3.ToFuture(), secondaryRequestGenerator3);
    Y_UNUSED(WaitFor(secondaryRequestGenerator3->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    hedgingDelay *= Config_->HedgingDelayTuneFactor;
    hedgingDelay /= *Config_->SecondaryRequestRatio;
    EXPECT_TRUE(AreDurationsEqual(NewHedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));

    auto promise4 = NewPromise<void>();
    auto secondaryRequestGenerator4 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise4.ToFuture(), secondaryRequestGenerator4);
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator4->GetHedgingWaitTimeFuture().IsSet());

    hedgingDelay = Config_->MaxHedgingDelay;
    EXPECT_TRUE(AreDurationsEqual(NewHedgingManager_->CollectStatistics().HedgingDelay, hedgingDelay));
}

TEST_F(TAdaptiveHedgingManagerTest, NewIncrementToken)
{
    Config_->MaxTokenCount = 1;
    Config_->SecondaryRequestRatio = 0.5;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);;
    CreateNewHedgingManager();

    auto promise1 = NewPromise<void>();
    auto secondaryRequestGenerator1 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise1.ToFuture(), secondaryRequestGenerator1);
    Y_UNUSED(WaitFor(secondaryRequestGenerator1->GetHedgingWaitTimeFuture())
        .ValueOrThrow());

    auto promise2 = NewPromise<void>();
    auto secondaryRequestGenerator2 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise2.ToFuture(), secondaryRequestGenerator2);
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    ASSERT_FALSE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    auto promise3 = NewPromise<void>();
    auto secondaryRequestGenerator3 = New<TTestSecondaryRequestGenerator>();
    NewHedgingManager_->RegisterRequest(promise3.ToFuture(), secondaryRequestGenerator3);

    // NB: Token from the third request let us hedge the second one.
    EXPECT_TRUE(secondaryRequestGenerator2->GetHedgingWaitTimeFuture().IsSet());

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    EXPECT_FALSE(secondaryRequestGenerator3->GetHedgingWaitTimeFuture().IsSet());

    EXPECT_TRUE(AreStatisticsEqual(
        NewHedgingManager_->CollectStatistics(),
        TNewHedgingManagerStatistics{
            .PrimaryRequestCount = 3,
            .SecondaryRequestCount = 2,
            .QueuedRequestCount = 2,
            .MaxQueueSize = 1,
            .HedgingDelay = Config_->MaxHedgingDelay,
        }));
}

TEST_F(TAdaptiveHedgingManagerTest, NewStress)
{
    Config_->MaxTokenCount = 10;
    Config_->MaxHedgingDelay = TDuration::MilliSeconds(100);

    CreateNewHedgingManager();

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

            NewHedgingManager_->RegisterRequest(request.Promise, request.Generator);
        }

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        for (auto index : requestIndexesWithSlowResponse) {
            requests[index].Promise.Set();
        }
    }

    auto statistics = NewHedgingManager_->CollectStatistics();
    EXPECT_EQ(statistics.PrimaryRequestCount, 100);
    // 10 initial tokens and 10 more generated by 100 requests.
    EXPECT_EQ(statistics.SecondaryRequestCount, 19);
    EXPECT_EQ(statistics.QueuedRequestCount, 70);
    EXPECT_GT(statistics.MaxQueueSize, 50);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
