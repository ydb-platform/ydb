#include "range_locker.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock::TRangeLock(TRangeLock&& other) noexcept
    : LockableRanges(std::move(other.LockableRanges))
    , Lsn(other.Lsn)
    , Range(other.Range)
    , Mask(other.Mask)
    , LockRange(other.LockRange)
    , Armed(other.Armed)
{
    other.Armed = false;
}

TRangeLock::~TRangeLock()
{
    Disarm();
}

TRangeLock& TRangeLock::operator=(TRangeLock&& other) noexcept
{
    Disarm();

    LockableRanges = std::move(other.LockableRanges);
    Lsn = other.Lsn;
    Range = other.Range;
    Mask = other.Mask;
    LockRange = other.LockRange;
    Armed = other.Armed;

    other.Armed = false;
    return *this;
}

void TRangeLock::Arm()
{
    if (Armed) {
        return;
    }
    Armed = true;

    if (auto lockableRanges = LockableRanges.lock()) {
        if (Lsn) {
            lockableRanges->LockPBuffer(Lsn);
        } else {
            Y_ABORT_UNLESS(!Mask.Empty());
            LockRange = lockableRanges->LockDDiskRange(Range, Mask);
        }
    }
}

void TRangeLock::Disarm()
{
    if (!Armed) {
        return;
    }
    Armed = false;

    if (auto lockableRanges = LockableRanges.lock()) {
        if (Lsn) {
            lockableRanges->UnlockPBuffer(Lsn);
        } else {
            lockableRanges->UnLockDDiskRange(LockRange);
        }
    }
}

TRangeLock::TRangeLock(ILockableRangesWeakPtr lockableRanges, ui64 lsn)
    : LockableRanges(std::move(lockableRanges))
    , Lsn(lsn)
{}

TRangeLock::TRangeLock(
    ILockableRangesWeakPtr lockableRanges,
    TBlockRange64 range,
    THostMask mask)
    : LockableRanges(std::move(lockableRanges))
    , Range(range)
    , Mask(mask)
{}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
