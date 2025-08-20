#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/recursive_spin_lock.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

namespace NYT::NThreading {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef NDEBUG

TEST(TSpinLockCountTest, SpinLock)
{
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
    TSpinLock lock;
    auto guard = Guard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    guard.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
}

TEST(TSpinLockCountTest, RecursiveSpinLock)
{
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
    TRecursiveSpinLock lock;
    auto guard1 = Guard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    auto guard2 = Guard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 2);
    guard2.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    guard1.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
}

TEST(TSpinLockCountTest, ForkAwareSpinLock)
{
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
    TForkAwareSpinLock lock;
    auto guard = Guard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    guard.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
}

TEST(TSpinLockCountTest, RWSpinLock)
{
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
    TReaderWriterSpinLock lock;
    auto readerGuard1 = ReaderGuard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    auto readerGuard2 = ReaderGuard(lock);
    EXPECT_EQ(GetActiveSpinLockCount(), 2);
    readerGuard1.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 1);
    readerGuard2.Release();
    EXPECT_EQ(GetActiveSpinLockCount(), 0);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NThreading
