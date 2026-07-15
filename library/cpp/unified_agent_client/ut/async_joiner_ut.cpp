#include <library/cpp/unified_agent_client/async_joiner.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NUnifiedAgent;

TEST(TAsyncJoiner, TryRefBeforeJoinReturnsTrue) {
    TAsyncJoiner joiner;
    EXPECT_TRUE(joiner.TryRef());
    joiner.UnRef();
    joiner.Join().Wait();
}

TEST(TAsyncJoiner, TryRefAfterJoinReturnsFalse) {
    TAsyncJoiner joiner;
    joiner.Join().Wait();
    EXPECT_FALSE(joiner.TryRef());
}
