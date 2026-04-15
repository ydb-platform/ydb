#include "range_locker.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRangeLockAccess
{
public:
    static TRangeLock Make(ILockableRanges* lockableRanges, ui64 lsn)
    {
        return TRangeLock(lockableRanges, lsn);
    }

    static TRangeLock Make(
        ILockableRanges* lockableRanges,
        TBlockRange64 range,
        TLocationMask mask)
    {
        return TRangeLock(lockableRanges, range, mask);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMockLockableRanges: public ILockableRanges
{
public:
    void LockPBuffer(ui64 lsn) override
    {
        ++LsnLocks[lsn];
    }

    void UnlockPBuffer(ui64 lsn) override
    {
        auto count = --LsnLocks[lsn];
        if (count == 0) {
            LsnLocks.erase(lsn);
        }
    }

    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        TLocationMask mask) override
    {
        Y_UNUSED(range);
        Y_UNUSED(mask);

        TLockRangeHandle handle = ++NextHandle;
        ++RangeLocks[handle];
        return handle;
    }

    void UnLockDDiskRange(TLockRangeHandle handle) override
    {
        auto count = --RangeLocks[handle];
        if (count == 0) {
            RangeLocks.erase(handle);
        }
    }

    TMap<ui64, size_t> LsnLocks;
    TMap<ui64, size_t> RangeLocks;

private:
    TLockRangeHandle NextHandle = 1000;
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeLockTest)
{
    Y_UNIT_TEST(TestNotArmed)
    {
        TMockLockableRanges mock;
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, 123);
            TRangeLock lock2 = TRangeLockAccess::Make(
                &mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestLsnLock)
    {
        TMockLockableRanges mock;

        {
            TRangeLock lock = TRangeLockAccess::Make(&mock, 123);

            lock.Arm();
            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks[123]);
            UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestRangeLockConstructor)
    {
        TMockLockableRanges mock;
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        {
            TRangeLock lock = TRangeLockAccess::Make(
                &mock,
                TBlockRange64::MakeOneBlock(100),
                mask);

            lock.Arm();
            UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks[1001]);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestMoveConstructor)
    {
        TMockLockableRanges mock;
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, 456);
            TRangeLock lock2 = TRangeLockAccess::Make(
                &mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            lock1.Arm();
            lock2.Arm();

            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());
            {
                TRangeLock lock3(std::move(lock1));
                TRangeLock lock4(std::move(lock2));
            }
            UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestMoveAssignment)
    {
        TMockLockableRanges mock;
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, 456);
            TRangeLock lock2 = TRangeLockAccess::Make(
                &mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            lock1.Arm();
            lock2.Arm();

            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());
            {
                TRangeLock lock3 = TRangeLockAccess::Make(&mock, 0);
                TRangeLock lock4 = TRangeLockAccess::Make(&mock, 0);
                lock3 = std::move(lock1);
                lock4 = std::move(lock2);
            }
            UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestDoubleArm)
    {
        TMockLockableRanges mock;
        TLocationMask mask = TLocationMask ::MakePrimaryDDisks();

        TRangeLock lock1 = TRangeLockAccess::Make(&mock, 456);
        TRangeLock lock2 = TRangeLockAccess::Make(
            &mock,
            TBlockRange64::MakeOneBlock(100),
            mask);
        lock1.Arm();
        lock2.Arm();

        UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());

        lock1.Arm();
        lock2.Arm();

        UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
