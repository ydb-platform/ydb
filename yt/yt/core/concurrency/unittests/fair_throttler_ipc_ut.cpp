#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_throttler_ipc.h>

#include <library/cpp/testing/common/env.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_
TEST(TFairThrottlerIpcTest, Test)
{
    auto path = TString(GetOutputPath() / "test_ipc");

    auto a = CreateFairThrottlerFileIpc(path);
    auto b = CreateFairThrottlerFileIpc(path);

    ASSERT_TRUE(a->TryLock());
    ASSERT_FALSE(b->TryLock());

    ASSERT_EQ(0u, a->ListBuckets().size());
    ASSERT_EQ(0u, b->ListBuckets().size());

    auto b0 = a->CreateBucket();

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
