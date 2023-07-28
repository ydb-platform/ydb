#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderWriterLockTest
    : public ::testing::Test
{
protected:
    TAsyncReaderWriterLock lock;
};

TEST_F(TAsyncReaderWriterLockTest, ReadMode)
{
    EXPECT_TRUE(lock.AcquireReader().IsSet());
    EXPECT_TRUE(lock.AcquireReader().IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(lock.AcquireReader().IsSet());

    lock.ReleaseReader();
    lock.ReleaseReader();
}

TEST_F(TAsyncReaderWriterLockTest, WriteMode)
{
    EXPECT_TRUE(lock.AcquireWriter().IsSet());

    auto firstFuture = lock.AcquireWriter();
    EXPECT_FALSE(firstFuture.IsSet());

    auto secondFuture = lock.AcquireWriter();
    EXPECT_FALSE(secondFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(secondFuture.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, WriteAfterRead)
{
    YT_UNUSED_FUTURE(lock.AcquireReader());

    auto future = lock.AcquireWriter();
    EXPECT_FALSE(future.IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(future.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, ServeWriteFirst)
{
    YT_UNUSED_FUTURE(lock.AcquireReader());

    auto writeFuture = lock.AcquireWriter();
    EXPECT_FALSE(writeFuture.IsSet());

    auto readFuture = lock.AcquireReader();
    EXPECT_FALSE(readFuture.IsSet());

    lock.ReleaseReader();
    EXPECT_TRUE(writeFuture.IsSet());
    EXPECT_FALSE(readFuture.IsSet());

    lock.ReleaseWriter();
    EXPECT_TRUE(readFuture.IsSet());

    lock.ReleaseReader();
}

TEST_F(TAsyncReaderWriterLockTest, CreateReaderGuard)
{
    auto firstGuard = TAsyncLockReaderGuard::Acquire(&lock);
    EXPECT_TRUE(firstGuard.IsSet());

    {
        auto secondGuard = TAsyncLockReaderGuard::Acquire(&lock);
        EXPECT_TRUE(secondGuard.IsSet());
    }

    auto thirdGuard = TAsyncLockReaderGuard::Acquire(&lock);
    EXPECT_TRUE(thirdGuard.IsSet());
}

TEST_F(TAsyncReaderWriterLockTest, CreateWriterGuard)
{
    auto guardFuture = TAsyncLockWriterGuard::Acquire(&lock);
    EXPECT_TRUE(guardFuture.IsSet());

    auto guard = guardFuture.Get().Value();

    auto future = lock.AcquireWriter();
    EXPECT_FALSE(future.IsSet());

    guard->Release();
    EXPECT_TRUE(future.IsSet());

    lock.ReleaseWriter();
}

TEST_F(TAsyncReaderWriterLockTest, ReleaseInDestructor)
{
    TFuture<void> future;
    {
        auto writeGuard = TAsyncLockWriterGuard::Acquire(&lock).Get().Value();
        future = lock.AcquireReader();
        EXPECT_FALSE(future.IsSet());
    }
    EXPECT_TRUE(future.IsSet());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
