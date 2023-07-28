#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/threading/recursive_spin_lock.h>
#include <library/cpp/yt/threading/event_count.h>

#include <thread>

namespace NYT::NThreading {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRecursiveSpinLockTest, SingleThread)
{
    TRecursiveSpinLock lock;
    EXPECT_FALSE(lock.IsLocked());
    EXPECT_TRUE(lock.TryAcquire());
    EXPECT_TRUE(lock.IsLocked());
    EXPECT_TRUE(lock.TryAcquire());
    EXPECT_TRUE(lock.IsLocked());
    lock.Release();
    EXPECT_TRUE(lock.IsLocked());
    lock.Release();
    EXPECT_FALSE(lock.IsLocked());
    EXPECT_TRUE(lock.TryAcquire());
    EXPECT_TRUE(lock.IsLocked());
    lock.Release();
    lock.Acquire();
    lock.Release();
}

TEST(TRecursiveSpinLockTest, TwoThreads)
{
    TRecursiveSpinLock lock;
    TEvent e1, e2, e3, e4, e5, e6, e7;

    std::thread t1([&] {
        e1.Wait();
        EXPECT_TRUE(lock.IsLocked());
        EXPECT_FALSE(lock.IsLockedByCurrentThread());
        EXPECT_FALSE(lock.TryAcquire());
        e2.NotifyOne();
        e3.Wait();
        EXPECT_TRUE(lock.IsLocked());
        EXPECT_FALSE(lock.IsLockedByCurrentThread());
        EXPECT_FALSE(lock.TryAcquire());
        e4.NotifyOne();
        e5.Wait();
        EXPECT_FALSE(lock.IsLocked());
        EXPECT_FALSE(lock.IsLockedByCurrentThread());
        EXPECT_TRUE(lock.TryAcquire());
        e6.NotifyOne();
        e7.Wait();
        lock.Release();
    });

    std::thread t2([&] {
        EXPECT_FALSE(lock.IsLocked());
        EXPECT_TRUE(lock.TryAcquire());
        EXPECT_TRUE(lock.IsLockedByCurrentThread());
        e1.NotifyOne();
        e2.Wait();
        EXPECT_TRUE(lock.TryAcquire());
        EXPECT_TRUE(lock.IsLockedByCurrentThread());
        e3.NotifyOne();
        e4.Wait();
        lock.Release();
        lock.Release();
        EXPECT_FALSE(lock.IsLocked());
        e5.NotifyOne();
        e6.Wait();
        EXPECT_TRUE(lock.IsLocked());
        EXPECT_FALSE(lock.IsLockedByCurrentThread());
        e7.NotifyOne();
        lock.Acquire();
        lock.Acquire();
        lock.Release();
        lock.Release();
    });

    t1.join();
    t2.join();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NThreading
