#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_throttler.h>
#include <yt/yt/core/concurrency/fair_throttler_ipc.h>

#include <library/cpp/testing/common/env.h>

#include <vector>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("FairThrottlerTest");

////////////////////////////////////////////////////////////////////////////////

TEST(TFairThrottlerDistributionTest, SingleWeight)
{
    for (double weight : std::vector<double>{1, 0.01, 100}) {
        ASSERT_EQ(IFairThrottler::ComputeFairDistribution(10, {weight}, {5}, {{}}), std::vector<i64>{5});
        ASSERT_EQ(IFairThrottler::ComputeFairDistribution(10, {weight}, {5}, {4}), std::vector<i64>{4});
        ASSERT_EQ(IFairThrottler::ComputeFairDistribution(10, {weight}, {12}, {{}}), std::vector<i64>{10});
    }
}

TEST(TFairThrottlerDistributionTest, TwoBuckets)
{
    ASSERT_EQ(IFairThrottler::ComputeFairDistribution(
        10,
        {1, 1},
        {5, 10},
        {{}, {}}),
        (std::vector<i64>{5, 5}));

    ASSERT_EQ(IFairThrottler::ComputeFairDistribution(
        10,
        {1, 1},
        {2, 10},
        {{}, {}}),
        (std::vector<i64>{2, 8}));

    ASSERT_EQ(IFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {10, 10},
        {{}, {}}),
        (std::vector<i64>{4, 6}));

    ASSERT_EQ(IFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {10, 10},
        {{}, {5}}),
        (std::vector<i64>{5, 5}));

    ASSERT_EQ(IFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {0, 10},
        {{}, {5}}),
        (std::vector<i64>{0, 5}));
}

////////////////////////////////////////////////////////////////////////////////

class TNonconcurrentFairThrottlerTest
    : public ::testing::Test
{
protected:
    TFairThrottlerConfigPtr Config_ = New<TFairThrottlerConfig>();
    TFairThrottlerBucketConfigPtr BucketConfig_ = New<TFairThrottlerBucketConfig>();
    IFairThrottlerPtr Throttler_;

    TNonconcurrentFairThrottlerTest()
    {
        Config_->TotalLimit = 100;

        auto logger = Logger().WithTag("Test: %v", ::testing::UnitTest::GetInstance()->current_test_info()->name());
        Throttler_ = CreateFairThrottler(Config_, logger, NProfiling::TProfiler{});
    }
};

TEST_F(TNonconcurrentFairThrottlerTest, SingleBucketNoRequests)
{
    Throttler_->CreateBucketThrottler("main", BucketConfig_);
    Sleep(TDuration::Seconds(1));
}

TEST_F(TNonconcurrentFairThrottlerTest, TwoBucketNoRequests)
{
    Throttler_->CreateBucketThrottler("first", BucketConfig_);
    Throttler_->CreateBucketThrottler("second", BucketConfig_);
    Sleep(TDuration::Seconds(1));
}

TEST_F(TNonconcurrentFairThrottlerTest, SingleBucketRequests)
{
    auto bucket = Throttler_->CreateBucketThrottler("main", BucketConfig_);

    std::vector<TFuture<void>> requests;

    int blocked = 0;
    for (int i = 0; i < 10; i++) {
        auto req = bucket->Throttle(10);
        blocked += req.IsSet() ? 0 : 1;

        Sleep(TDuration::MilliSeconds(200));
        requests.push_back(req);
    }

    ASSERT_LE(blocked, 2);
}

int CountCompleted(const std::vector<TFuture<void>>& requests)
{
    int complete = 0;
    for (const auto& req : requests) {
        if (req.IsSet()) {
            complete++;
        }
    }
    return complete;
}

TEST_F(TNonconcurrentFairThrottlerTest, EvenDistributionTwoBuckets)
{
    auto first = Throttler_->CreateBucketThrottler("first", BucketConfig_);
    auto second = Throttler_->CreateBucketThrottler("second", BucketConfig_);

    std::vector<TFuture<void>> firstRequests, secondRequests;

    for (int i = 0; i < 100; i++) {
        firstRequests.push_back(first->Throttle(10));
        secondRequests.push_back(second->Throttle(10));
    }

    Sleep(TDuration::Seconds(5));

    auto firstComplete = CountCompleted(firstRequests);
    auto secondComplete = CountCompleted(secondRequests);

    ASSERT_GE(firstComplete, 20);
    ASSERT_GE(secondComplete, 20);
    ASSERT_LE(firstComplete, 30);
    ASSERT_LE(secondComplete, 30);
}

TEST_F(TNonconcurrentFairThrottlerTest, BucketWeight)
{
    auto first = Throttler_->CreateBucketThrottler("light", BucketConfig_);

    auto heavyConfig = New<TFairThrottlerBucketConfig>();
    heavyConfig->Weight = 4;

    auto second = Throttler_->CreateBucketThrottler("heavy", heavyConfig);

    std::vector<TFuture<void>> firstRequests, secondRequests;

    for (int i = 0; i < 100; i++) {
        firstRequests.push_back(first->Throttle(10));
        secondRequests.push_back(second->Throttle(10));
    }

    Sleep(TDuration::Seconds(5));

    auto firstComplete = CountCompleted(firstRequests);
    auto secondComplete = CountCompleted(secondRequests);

    ASSERT_GE(firstComplete, 8);
    ASSERT_GE(secondComplete, 35);
    ASSERT_LE(firstComplete, 12);
    ASSERT_LE(secondComplete, 45);
}

TEST_F(TNonconcurrentFairThrottlerTest, BucketLimit)
{
    auto unlimitedBucket = Throttler_->CreateBucketThrottler("first", BucketConfig_);

    auto limitedConfig = New<TFairThrottlerBucketConfig>();
    limitedConfig->RelativeLimit = 0.5;
    limitedConfig->Weight = 100;

    auto limitedBucket = Throttler_->CreateBucketThrottler("second", limitedConfig);

    std::vector<TFuture<void>> unlimitedRequests, limitedRequest;

    for (int i = 0; i < 100; i++) {
        unlimitedRequests.push_back(unlimitedBucket->Throttle(10));
        limitedRequest.push_back(limitedBucket->Throttle(10));
    }

    Sleep(TDuration::Seconds(5));

    auto unlimitedComplete = CountCompleted(unlimitedRequests);
    auto limitedComplete = CountCompleted(limitedRequest);

    ASSERT_GE(unlimitedComplete, 20);
    ASSERT_GE(limitedComplete, 20);
    ASSERT_LE(unlimitedComplete, 30);
    ASSERT_LE(limitedComplete, 30);
}

TEST_F(TNonconcurrentFairThrottlerTest, Cancel)
{
    auto bucket = Throttler_->CreateBucketThrottler("main", BucketConfig_);

    std::vector<TFuture<void>> cancelledRequests;
    for (int i = 0; i < 100; i++) {
        cancelledRequests.push_back(bucket->Throttle(10));
    }

    ASSERT_FALSE(bucket->TryAcquire(1));

    for (const auto& req : cancelledRequests) {
        req.Cancel(TError("Cancel"));
    }

    Sleep(TDuration::MilliSeconds(500));

    ASSERT_TRUE(bucket->TryAcquire(1));
}

TEST_F(TNonconcurrentFairThrottlerTest, AcquireOverdraft)
{
    auto bucket = Throttler_->CreateBucketThrottler("main", BucketConfig_);

    for (int i = 0; i < 100; i++) {
        bucket->Acquire(5);
    }

    for (int i = 0; i < 3; i++) {
        ASSERT_FALSE(bucket->TryAcquire(1));
        ASSERT_TRUE(bucket->IsOverdraft());

        Sleep(TDuration::Seconds(1));
    }

    Sleep(TDuration::Seconds(5));
    ASSERT_TRUE(bucket->TryAcquire(1));
    ASSERT_FALSE(bucket->IsOverdraft());
}

TEST_F(TNonconcurrentFairThrottlerTest, AcquireLimitedBucket)
{
    auto limitedConfig = New<TFairThrottlerBucketConfig>();
    limitedConfig->RelativeLimit = 0.5;

    auto bucket = Throttler_->CreateBucketThrottler("main", limitedConfig);

    Sleep(TDuration::Seconds(5));
    ASSERT_TRUE(bucket->TryAcquire(1));
    ASSERT_TRUE(bucket->TryAcquire(1));
}

TEST_F(TNonconcurrentFairThrottlerTest, TryAcquireAvailable)
{
    auto bucket = Throttler_->CreateBucketThrottler("main", BucketConfig_);

    Sleep(TDuration::Seconds(1));
    ASSERT_EQ(bucket->TryAcquireAvailable(150), 100);
    ASSERT_EQ(bucket->GetQueueTotalAmount(), 0);
    ASSERT_GE(bucket->GetAvailable(), 0);
}

TEST_F(TNonconcurrentFairThrottlerTest, Release)
{
    auto bucket = Throttler_->CreateBucketThrottler("main", BucketConfig_);

    Sleep(TDuration::Seconds(5));

    ASSERT_EQ(bucket->GetAvailable(), 100);

    ASSERT_TRUE(bucket->TryAcquire(100));
    bucket->Release(100);
    ASSERT_TRUE(bucket->TryAcquire(100));
    ASSERT_FALSE(bucket->TryAcquire(100));
}

////////////////////////////////////////////////////////////////////////////////

class TConcurrentFairThrottlerTest
    : public ::testing::Test
{
protected:
    TFairThrottlerConfigPtr Config_ = New<TFairThrottlerConfig>();
    TFairThrottlerBucketConfigPtr BucketConfig_ = New<TFairThrottlerBucketConfig>();

    IFairThrottlerPtr DatNodeThrottler_;
    IFairThrottlerPtr ExeNodeThrottler_;

    TConcurrentFairThrottlerTest()
    {
        std::string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();

        Config_->IpcPath = TString(GetOutputPath() / (testName + ".throttler"));
        Config_->TotalLimit = 100;

        auto logger = Logger().WithTag("Test: %v", testName);

        DatNodeThrottler_ = CreateFairThrottler(Config_, logger.WithTag("Node: dat"), NProfiling::TProfiler{});
        ExeNodeThrottler_ = CreateFairThrottler(Config_, logger.WithTag("Node: exe"), NProfiling::TProfiler{});
    }
};

TEST_F(TConcurrentFairThrottlerTest, TwoBuckets)
{
    auto first = DatNodeThrottler_->CreateBucketThrottler("first", BucketConfig_);
    auto second = ExeNodeThrottler_->CreateBucketThrottler("second", BucketConfig_);

    std::vector<TFuture<void>> firstRequests, secondRequests;

    for (int i = 0; i < 100; i++) {
        firstRequests.push_back(first->Throttle(10));
        secondRequests.push_back(second->Throttle(10));
    }

    Sleep(TDuration::Seconds(5));

    auto firstComplete = CountCompleted(firstRequests);
    auto secondComplete = CountCompleted(secondRequests);

    ASSERT_GE(firstComplete, 20);
    ASSERT_GE(secondComplete, 20);
    ASSERT_LE(firstComplete, 30);
    ASSERT_LE(secondComplete, 30);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
