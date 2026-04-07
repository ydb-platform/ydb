#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_throttler_ipc.h>

#include <library/cpp/testing/common/env.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Test");

class TFairThrottlerIpcTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>>
{ };

TEST_P(TFairThrottlerIpcTest, Test)
{
    auto path = TString(GetOutputPath() / ToString(TGuid::Create()));

    auto [useShmemA, useShmemB] = GetParam();
    auto a = CreateFairThrottlerFileIpc(path, useShmemA, Logger);
    auto b = CreateFairThrottlerFileIpc(path, useShmemB, Logger);

    ASSERT_TRUE(a->TryLock());
    ASSERT_FALSE(b->TryLock());

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(0u, b->ListBuckets().size());

    auto b0 = a->CreateBucket();

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(1u, b->ListBuckets().size());

    auto b1 = b->ListBuckets()[0];
    for (int i = 0; i < 100; i++) {
        b0->GetState()->Limit.store(i);
        EXPECT_EQ(b0->GetState()->Limit.load(), i);
    }

    b0.Reset();

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(0u, b->ListBuckets().size());
}

INSTANTIATE_TEST_SUITE_P(
    TFairThrottlerIpcTest,
    TFairThrottlerIpcTest,
    ::testing::Values(
        std::tuple(false, false),
        std::tuple(false,  true),
        std::tuple( true, false),
        std::tuple( true,  true)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
