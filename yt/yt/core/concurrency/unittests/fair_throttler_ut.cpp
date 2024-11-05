#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_throttler.h>

#include <library/cpp/testing/common/env.h>

#include <vector>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFairThrottlerDistributionTest, SingleWeight)
{
    for (double weight : std::vector<double>{1, 0.01, 100}) {
        ASSERT_EQ(TFairThrottler::ComputeFairDistribution(10, {weight}, {5}, {{}}), std::vector<i64>{5});
        ASSERT_EQ(TFairThrottler::ComputeFairDistribution(10, {weight}, {5}, {4}), std::vector<i64>{4});
        ASSERT_EQ(TFairThrottler::ComputeFairDistribution(10, {weight}, {12}, {{}}), std::vector<i64>{10});
    }
}

TEST(TFairThrottlerDistributionTest, TwoBuckets)
{
    ASSERT_EQ(TFairThrottler::ComputeFairDistribution(
        10,
        {1, 1},
        {5, 10},
        {{}, {}}),
        (std::vector<i64>{5, 5}));

    ASSERT_EQ(TFairThrottler::ComputeFairDistribution(
        10,
        {1, 1},
        {2, 10},
        {{}, {}}),
        (std::vector<i64>{2, 8}));

    ASSERT_EQ(TFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {10, 10},
        {{}, {}}),
        (std::vector<i64>{4, 6}));

    ASSERT_EQ(TFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {10, 10},
        {{}, {5}}),
        (std::vector<i64>{5, 5}));

    ASSERT_EQ(TFairThrottler::ComputeFairDistribution(
        10,
        {2, 3},
        {0, 10},
        {{}, {5}}),
        (std::vector<i64>{0, 5}));
}

static NLogging::TLogger Logger{"FairThrottlerTest"};

struct TFairThrottlerTest
    : public ::testing::Test
{
    TFairThrottlerConfigPtr Config = New<TFairThrottlerConfig>();
    TFairThrottlerBucketConfigPtr BucketConfig = New<TFairThrottlerBucketConfig>();
    TFairThrottlerPtr FairThrottler;

    TFairThrottlerTest()
    {
        Config->TotalLimit = 100;

        auto logger = Logger().WithTag("Test: %v", ::testing::UnitTest::GetInstance()->current_test_info()->name());
        FairThrottler = New<TFairThrottler>(Config, logger, NProfiling::TProfiler{});
    }
};

TEST_F(TFairThrottlerTest, SingleBucketNoRequests)
{
    FairThrottler->CreateBucketThrottler("main", BucketConfig);
    Sleep(TDuration::Seconds(1));
}

TEST_F(TFairThrottlerTest, TwoBucketNoRequests)
{
    FairThrottler->CreateBucketThrottler("first", BucketConfig);
    FairThrottler->CreateBucketThrottler("second", BucketConfig);
    Sleep(TDuration::Seconds(1));
}

TEST_F(TFairThrottlerTest, SingleBucketRequests)
{
    auto bucket = FairThrottler->CreateBucketThrottler("main", BucketConfig);

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

TEST_F(TFairThrottlerTest, EvenDistributionTwoBuckets)
{
    auto first = FairThrottler->CreateBucketThrottler("first", BucketConfig);
    auto second = FairThrottler->CreateBucketThrottler("second", BucketConfig);

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

TEST_F(TFairThrottlerTest, BucketWeight)
{
    auto first = FairThrottler->CreateBucketThrottler("light", BucketConfig);

    auto heavyConfig = New<TFairThrottlerBucketConfig>();
    heavyConfig->Weight = 4;

    auto second = FairThrottler->CreateBucketThrottler("heavy", heavyConfig);

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

TEST_F(TFairThrottlerTest, BucketLimit)
{
    auto unlimitedBucket = FairThrottler->CreateBucketThrottler("first", BucketConfig);

    auto limitedConfig = New<TFairThrottlerBucketConfig>();
    limitedConfig->RelativeLimit = 0.5;
    limitedConfig->Weight = 100;

    auto limitedBucket = FairThrottler->CreateBucketThrottler("second", limitedConfig);

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

TEST_F(TFairThrottlerTest, Cancel)
{
    auto bucket = FairThrottler->CreateBucketThrottler("main", BucketConfig);

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

TEST_F(TFairThrottlerTest, AcquireOverdraft)
{
    auto bucket = FairThrottler->CreateBucketThrottler("main", BucketConfig);

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

TEST_F(TFairThrottlerTest, AcquireLimitedBucket)
{
    auto limitedConfig = New<TFairThrottlerBucketConfig>();
    limitedConfig->RelativeLimit = 0.5;

    auto bucket = FairThrottler->CreateBucketThrottler("main", limitedConfig);

    Sleep(TDuration::Seconds(5));
    ASSERT_TRUE(bucket->TryAcquire(1));
    ASSERT_TRUE(bucket->TryAcquire(1));
}

TEST_F(TFairThrottlerTest, TryAcquireAvailable)
{
    auto bucket = FairThrottler->CreateBucketThrottler("main", BucketConfig);

    Sleep(TDuration::Seconds(1));
    ASSERT_EQ(bucket->TryAcquireAvailable(150), 100);
    ASSERT_EQ(bucket->GetQueueTotalAmount(), 0);
    ASSERT_GE(bucket->GetAvailable(), 0);
}

TEST_F(TFairThrottlerTest, Release)
{
    auto bucket = FairThrottler->CreateBucketThrottler("main", BucketConfig);

    Sleep(TDuration::Seconds(5));

    ASSERT_EQ(bucket->GetAvailable(), 100);

    ASSERT_TRUE(bucket->TryAcquire(100));
    bucket->Release(100);
    ASSERT_TRUE(bucket->TryAcquire(100));
    ASSERT_FALSE(bucket->TryAcquire(100));
}

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerIPCTest
    : public ::testing::Test
{
    TFairThrottlerConfigPtr Config = New<TFairThrottlerConfig>();
    TFairThrottlerBucketConfigPtr BucketConfig = New<TFairThrottlerBucketConfig>();

    TFairThrottlerPtr DatNode, ExeNode;

    TFairThrottlerIPCTest()
    {
        TString testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();

        Config->IPCPath = GetOutputPath() / (testName + ".throttler");
        Config->TotalLimit = 100;

        auto logger = Logger().WithTag("Test: %v", testName);

        DatNode = New<TFairThrottler>(Config, logger.WithTag("Node: dat"), NProfiling::TProfiler{});
        ExeNode = New<TFairThrottler>(Config, logger.WithTag("Node: exe"), NProfiling::TProfiler{});
    }
};

TEST_F(TFairThrottlerIPCTest, TwoBucket)
{
    auto first = DatNode->CreateBucketThrottler("first", BucketConfig);
    auto second = ExeNode->CreateBucketThrottler("second", BucketConfig);

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

#ifndef _win_
TEST(TFileIPC, Test)
{
    auto path = GetOutputPath() / "test_ipc";

    auto a = CreateFileThrottlerIPC(path);
    auto b = CreateFileThrottlerIPC(path);

    ASSERT_TRUE(a->TryLock());
    ASSERT_FALSE(b->TryLock());

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(0u, b->ListBuckets().size());

    auto b0 = a->AddBucket();

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(1u, b->ListBuckets().size());

    b0.Reset();

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(0u, b->ListBuckets().size());
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
