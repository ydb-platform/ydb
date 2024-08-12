#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/parallel_runner.h>

#include <atomic>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSyncParallelRunnerTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{ };

TEST_P(TSyncParallelRunnerTest, Do)
{
    auto [count, expectedSum] = GetParam();
    auto runner = TParallelRunner<int>::CreateSync();
    for (int index = 0; index < count; ++index) {
        runner.Add(index);
    }
    int sum = 0;
    auto future = runner.Run([&] (int arg) {
        sum += arg;
    });
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().IsOK());
    EXPECT_EQ(sum, expectedSum);
}

INSTANTIATE_TEST_SUITE_P(
    TSyncParallelRunnerTest,
    TSyncParallelRunnerTest,
    ::testing::Values(
        std::tuple( 0,  0),
        std::tuple(10, 45)));

////////////////////////////////////////////////////////////////////////////////

class TAsyncParallelRunnerTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, int>>
{
protected:
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("Queue");
};

TEST_P(TAsyncParallelRunnerTest, Do)
{
    auto [batchSize, count, expectedSum] = GetParam();
    auto runner = TParallelRunner<int>::CreateAsync(ActionQueue_->GetInvoker(), batchSize);
    for (int index = 0; index < count; ++index) {
        runner.Add(index);
    }
    std::atomic<int> sum = 0;
    runner.Run([&] (int arg) {
        sum += arg;
    })
        .Get()
        .ThrowOnError();
    EXPECT_EQ(sum.load(), expectedSum);
}

INSTANTIATE_TEST_SUITE_P(
    TAsyncParallelRunnerTest,
    TAsyncParallelRunnerTest,
    ::testing::Values(
        std::tuple(1,  0,  0),
        std::tuple(3, 10, 45),
        std::tuple(3,  9, 36)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT:::NConcurrency
