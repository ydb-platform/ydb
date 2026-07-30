#include "range_locker.h"

#include "record_id_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRangeLockAccess
{
public:
    static TRangeLock Make(ILockableRanges* lockableRanges, TRecordId recordId)
    {
        return TRangeLock(lockableRanges, recordId);
    }

    static TRangeLock
    Make(ILockableRanges* lockableRanges, TBlockRange64 range, THostMask mask)
    {
        return TRangeLock(lockableRanges, range, mask);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMockLockableRanges: public ILockableRanges
{
public:
    void LockPBuffer(TRecordId recordId) override
    {
        ++LsnLocks[recordId];
    }

    void UnlockPBuffer(TRecordId recordId) override
    {
        auto count = --LsnLocks[recordId];
        if (count == 0) {
            LsnLocks.erase(recordId);
        }
    }

    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        THostMask mask) override
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

    TMap<TRecordId, size_t> LsnLocks;
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
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, MakeId(123));
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
            TRangeLock lock = TRangeLockAccess::Make(&mock, MakeId(123));

            lock.Arm();
            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks[MakeId(123)]);
            UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock.LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock.RangeLocks.size());
    }

    Y_UNIT_TEST(TestRangeLockConstructor)
    {
        TMockLockableRanges mock;
        THostMask mask = THostMask::MakeAll(3);

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
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, MakeId(456));
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
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(&mock, MakeId(456));
            TRangeLock lock2 = TRangeLockAccess::Make(
                &mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            lock1.Arm();
            lock2.Arm();

            UNIT_ASSERT_VALUES_EQUAL(1, mock.LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock.RangeLocks.size());
            {
                TRangeLock lock3 = TRangeLockAccess::Make(&mock, MakeId(0));
                TRangeLock lock4 = TRangeLockAccess::Make(&mock, MakeId(0));
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
        THostMask mask = THostMask::MakeAll(3);

        TRangeLock lock1 = TRangeLockAccess::Make(&mock, MakeId(456));
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
