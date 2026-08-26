#include "range_locker.h"

#include "pbuffer_key_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRangeLockAccess
{
public:
    static TRangeLock Make(
        ILockableRangesWeakPtr lockableRanges,
        TPBufferKey pBufferKey)
    {
        return TRangeLock(std::move(lockableRanges), pBufferKey);
    }

    static TRangeLock Make(
        ILockableRangesWeakPtr lockableRanges,
        TBlockRange64 range,
        THostMask mask)
    {
        return TRangeLock(std::move(lockableRanges), range, mask);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMockLockableRanges
    : public ILockableRanges
    , public std::enable_shared_from_this<TMockLockableRanges>
{
public:
    void LockPBuffer(TPBufferKey pBufferKey) override
    {
        ++LsnLocks[pBufferKey];
    }

    void UnlockPBuffer(TPBufferKey pBufferKey) override
    {
        auto count = --LsnLocks[pBufferKey];
        if (count == 0) {
            LsnLocks.erase(pBufferKey);
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

    TMap<TPBufferKey, size_t> LsnLocks;
    TMap<ui64, size_t> RangeLocks;

private:
    TLockRangeHandle NextHandle = 1000;
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeLockTest)
{
    Y_UNIT_TEST(TestNotArmed)
    {
        auto mock = std::make_shared<TMockLockableRanges>();
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(mock, MakeKey(123));
            TRangeLock lock2 = TRangeLockAccess::Make(
                mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
    }

    Y_UNIT_TEST(TestLsnLock)
    {
        auto mock = std::make_shared<TMockLockableRanges>();

        {
            TRangeLock lock = TRangeLockAccess::Make(mock, MakeKey(123));

            lock.Arm();
            UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks[MakeKey(123)]);
            UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
    }

    Y_UNIT_TEST(TestRangeLockConstructor)
    {
        auto mock = std::make_shared<TMockLockableRanges>();
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock = TRangeLockAccess::Make(
                mock,
                TBlockRange64::MakeOneBlock(100),
                mask);

            lock.Arm();
            UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks[1001]);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
    }

    Y_UNIT_TEST(TestMoveConstructor)
    {
        auto mock = std::make_shared<TMockLockableRanges>();
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(mock, MakeKey(456));
            TRangeLock lock2 = TRangeLockAccess::Make(
                mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            lock1.Arm();
            lock2.Arm();

            UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks.size());
            {
                TRangeLock lock3(std::move(lock1));
                TRangeLock lock4(std::move(lock2));
            }
            UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
    }

    Y_UNIT_TEST(TestMoveAssignment)
    {
        auto mock = std::make_shared<TMockLockableRanges>();
        THostMask mask = THostMask::MakeAll(3);

        {
            TRangeLock lock1 = TRangeLockAccess::Make(mock, MakeKey(456));
            TRangeLock lock2 = TRangeLockAccess::Make(
                mock,
                TBlockRange64::MakeOneBlock(100),
                mask);
            lock1.Arm();
            lock2.Arm();

            UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks.size());
            {
                TRangeLock lock3 = TRangeLockAccess::Make(mock, MakeKey(0));
                TRangeLock lock4 = TRangeLockAccess::Make(mock, MakeKey(0));
                lock3 = std::move(lock1);
                lock4 = std::move(lock2);
            }
            UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
            UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, mock->RangeLocks.size());
    }

    Y_UNIT_TEST(TestDoubleArm)
    {
        auto mock = std::make_shared<TMockLockableRanges>();
        THostMask mask = THostMask::MakeAll(3);

        TRangeLock lock1 = TRangeLockAccess::Make(mock, MakeKey(456));
        TRangeLock lock2 = TRangeLockAccess::Make(
            mock,
            TBlockRange64::MakeOneBlock(100),
            mask);
        lock1.Arm();
        lock2.Arm();

        UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks.size());

        lock1.Arm();
        lock2.Arm();

        UNIT_ASSERT_VALUES_EQUAL(1, mock->LsnLocks.size());
        UNIT_ASSERT_VALUES_EQUAL(1, mock->RangeLocks.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
