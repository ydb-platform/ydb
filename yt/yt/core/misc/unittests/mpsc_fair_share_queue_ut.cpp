#include <yt/yt/core/misc/mpsc_fair_share_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <util/random/shuffle.h>

#include <numeric>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMockTask final
{
    explicit TMockTask(int taskId)
        : TaskId(taskId)
    { }

    int TaskId = 0;
};

using TMockTaskPtr = TIntrusivePtr<TMockTask>;

using TTestFairShareQueue = TMpscFairShareQueue<int, TMockTaskPtr, std::string>;

template<typename TPoolId, typename TItem, typename TFairShareTag>
std::vector<TItem> DequeueMany(int count, TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>& queue)
{
    std::vector<TItem> result;
    result.reserve(count);

    queue.PrepareDequeue();

    for (int i = 0; i < count; ++i) {
        if (auto task = queue.TryDequeue()) {
            result.push_back(std::move(task));
        } else {
            break;
        }
    }

    return result;
}

template<typename TPoolId, typename TItem, typename TFairShareTag>
void MarkFinishedMany(TMpscFairShareQueue<TPoolId, TItem, TFairShareTag>& queue, const std::vector<TItem>& requests)
{
    auto now = GetCpuInstant();

    for (const auto& request :  requests) {
        queue.MarkFinished(request, now);
    }
}

std::string ToString(const THashSet<int>& expected)
{
    std::ostringstream stream;

    for (auto x : expected) {
        stream << x << ",";
    }

    return stream.str();
}

bool CheckEqual(const auto& tasks, const THashSet<int>& expected)
{
    THashSet<int> actual;
    for (const auto& task : tasks) {
        actual.insert(task->TaskId);
    }

    EXPECT_TRUE(actual == expected) <<
        "Expected: {" << ToString(expected) <<
        "} Actual: {" << ToString(actual) << "}";

    return actual == expected;
}

TEST(TMpscFairShareQueueTest, Simple)
{
    TTestFairShareQueue queue;

    queue.Enqueue({
        .Item = New<TMockTask>(1),
        .PoolId = 1,
        .PoolWeight = 4,
        .FairShareTag = "request_1",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(2),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_3",
    });

    std::vector<TMockTaskPtr> running;
    {
        auto tasks = DequeueMany(2, queue);
        EXPECT_TRUE(CheckEqual(tasks, {1, 2}));
        running.insert(running.end(), tasks.begin(), tasks.end());
    }

    {
        auto tasks = DequeueMany(2, queue);
        EXPECT_EQ(std::ssize(tasks), 0);
    }

    Sleep(TDuration::MilliSeconds(10));

    queue.Enqueue({
        .Item = New<TMockTask>(5),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_3",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(4),
        .PoolId = 1,
        .PoolWeight = 4,
        .FairShareTag = "request_2",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(3),
        .PoolId = 1,
        .PoolWeight = 4,
        .FairShareTag = "request_1",
    });

    auto tasks = DequeueMany(2, queue);
    EXPECT_TRUE(CheckEqual(tasks, {3, 4}));
    running.insert(running.end(), tasks.begin(), tasks.end());

    MarkFinishedMany(queue, running);

    tasks = DequeueMany(10, queue);
    EXPECT_TRUE(CheckEqual(tasks, {5}));
    MarkFinishedMany(queue, tasks);

    tasks = DequeueMany(100, queue);
    MarkFinishedMany(queue, tasks);

    queue.Cleanup();
    EXPECT_EQ(0, queue.GetPoolCount());
}

TEST(TMpscFairShareQueueTest, StdSmartPtr)
{
    TMpscFairShareQueue<int, std::unique_ptr<TMockTask>, std::string> queue;

    queue.Enqueue({
        .Item = std::make_unique<TMockTask>(1),
        .PoolId = 1,
        .PoolWeight = 4,
        .FairShareTag = "request_1",
    });

    queue.Enqueue({
        .Item = std::make_unique<TMockTask>(2),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_3",
    });

    auto tasks = DequeueMany(2, queue);
    EXPECT_TRUE(CheckEqual(tasks, {1, 2}));

    MarkFinishedMany(queue, tasks);
}

TEST(TMpscFairShareQueueTest, FairNewBucket)
{
    TTestFairShareQueue queue;

    queue.Enqueue({
        .Item = New<TMockTask>(1),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_1",
    });

    std::vector<TMockTaskPtr> running;
    {
        auto tasks = DequeueMany(2, queue);
        EXPECT_TRUE(CheckEqual(tasks, {1}));
        running.push_back(tasks.front());
    }

    Sleep(TDuration::MilliSeconds(100));

    queue.Enqueue({
        .Item = New<TMockTask>(2),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_1",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(3),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });
    queue.Enqueue({
        .Item = New<TMockTask>(4),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    {
        auto tasks = DequeueMany(2, queue);
        EXPECT_TRUE(CheckEqual(tasks, {3, 4}));
        running.insert(running.end(), tasks.begin(), tasks.end());
    }

    Sleep(TDuration::MilliSeconds(10));

    queue.Enqueue({
        .Item = New<TMockTask>(6),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(7),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    {
        auto tasks = DequeueMany(1, queue);
        EXPECT_TRUE(CheckEqual(tasks, {2}));
        running.insert(running.end(), tasks.begin(), tasks.end());
    }

    auto tasks = DequeueMany(10, queue);
    EXPECT_TRUE(CheckEqual(tasks, {6, 7}));
    MarkFinishedMany(queue, tasks);

    MarkFinishedMany(queue, running);

    tasks = DequeueMany(100, queue);
    MarkFinishedMany(queue, tasks);

    queue.Cleanup();
    EXPECT_EQ(0, queue.GetPoolCount());
}

TEST(TMpscFairShareQueueTest, FairNewPool)
{
    TTestFairShareQueue queue;

    queue.Enqueue({
        .Item = New<TMockTask>(1),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_1",
    });

    std::vector<TMockTaskPtr> running;
    {
        auto tasks = DequeueMany(2, queue);
        EXPECT_TRUE(CheckEqual(tasks, {1}));
        running.push_back(tasks.front());
    }

    queue.Enqueue({
        .Item = New<TMockTask>(2),
        .PoolId = 1,
        .PoolWeight = 1,
        .FairShareTag = "request_1",
    });

    Sleep(TDuration::MilliSeconds(100));
    queue.Enqueue({
        .Item = New<TMockTask>(3),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });
    queue.Enqueue({
        .Item = New<TMockTask>(4),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    {
        auto tasks = DequeueMany(1, queue);
        EXPECT_TRUE(CheckEqual(tasks, {3}));
        running.insert(running.end(), tasks.begin(), tasks.end());
    }

    Sleep(TDuration::MilliSeconds(10));

    queue.Enqueue({
        .Item = New<TMockTask>(6),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    queue.Enqueue({
        .Item = New<TMockTask>(7),
        .PoolId = 2,
        .PoolWeight = 1,
        .FairShareTag = "request_2",
    });

    {
        auto tasks = DequeueMany(1, queue);
        EXPECT_TRUE(CheckEqual(tasks, {4}));
        running.insert(running.end(), tasks.begin(), tasks.end());
    }
    auto tasks = DequeueMany(10, queue);
    EXPECT_TRUE(CheckEqual(tasks, {6, 7, 2}));
    MarkFinishedMany(queue, tasks);

    MarkFinishedMany(queue, running);

    tasks = DequeueMany(100, queue);
    MarkFinishedMany(queue, tasks);

    queue.Cleanup();
    EXPECT_EQ(0, queue.GetPoolCount());
}

TEST(TMpscFairShareQueueTest, Bench)
{
    const int TaskCount = 1'000'000;
    const int PoolsCount = 10;
    const int BucketsCount = 100;

    using TTestFairShareQueue = TMpscFairShareQueue<int, i64, int>;

    TTestFairShareQueue queue;

    int expectedRunningCount = 4096;

    std::vector<i64> running;
    int tasksIndex = 1;
    while (tasksIndex < TaskCount) {
        int tasksToSchedule = RandomNumber<ui32>(expectedRunningCount);
        for (int i = 0; i < tasksToSchedule; ++i) {
            queue.Enqueue({
                .Item = tasksIndex,
                .PoolId = tasksIndex % PoolsCount,
                .PoolWeight = double(tasksIndex % PoolsCount) + 1,
                .FairShareTag = int(RandomNumber<ui32>(BucketsCount)),
            });
            ++tasksIndex;
        }

        int dequeCount = RandomNumber<ui32>(expectedRunningCount);
        auto tasks = DequeueMany(dequeCount, queue);
        running.insert(running.end(), tasks.begin(), tasks.end());

        auto finishedCount = RandomNumber<ui32>(expectedRunningCount);
        {
            Shuffle(running.begin(), running.end());
            int splitSize = std::min<ui32>(finishedCount, std::ssize(running));
            std::vector<i64> finished(running.begin() + splitSize, running.end());
            running.resize(std::ssize(running) - std::ssize(finished));

            YT_VERIFY(std::ssize(running) == splitSize);
            MarkFinishedMany(queue, finished);
        }
    }

    EXPECT_TRUE(std::ssize(running) < expectedRunningCount * 10);
    MarkFinishedMany(queue, running);

    auto tasks = DequeueMany(TaskCount, queue);
    MarkFinishedMany(queue, tasks);

    queue.Cleanup();
    EXPECT_EQ(0, queue.GetPoolCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
