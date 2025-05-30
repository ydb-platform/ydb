#include <yt/yt/library/procfs/procfs.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/test_framework/framework.h>

#include <thread>

namespace NYT::NProcFS {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TProcFSTest
    : public ::testing::Test
{ };

TEST_F(TProcFSTest, GetThreadCount)
{
    int threadCount = GetThreadCount();
    EXPECT_GE(threadCount, 1);
    EXPECT_LE(threadCount, 300);
}

TEST_F(TProcFSTest, ThreadCountIncreasedOnThreadSpawning)
{
    int threadCount = GetThreadCount();

    auto promise = NewPromise<void>();

    std::thread thread([future = promise.ToFuture()] {
        future.Get();
    });

    int newThreadCount = GetThreadCount();
    EXPECT_EQ(newThreadCount, threadCount + 1);
    promise.Set();
    thread.join();
}

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProcFS
