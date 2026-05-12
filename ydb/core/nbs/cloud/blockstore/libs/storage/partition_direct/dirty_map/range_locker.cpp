#include "range_locker.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock::TRangeLock(TRangeLock&& other) noexcept
    : LockableRanges(other.LockableRanges)
    , Lsn(other.Lsn)
    , Range(other.Range)
    , Mask(other.Mask)
    , LockRange(other.LockRange)
    , Armed(other.Armed)
{
    other.Armed = false;
    other.LockableRanges = nullptr;
}

TRangeLock::~TRangeLock()
{
    if (!Armed) {
        return;
    }

    Y_ABORT_UNLESS(LockableRanges);

    if (Lsn) {
        LockableRanges->UnlockPBuffer(Lsn);
    } else {
        LockableRanges->UnLockDDiskRange(LockRange);
    }
}

TRangeLock& TRangeLock::operator=(TRangeLock&& other) noexcept
{
    LockableRanges = other.LockableRanges;
    Lsn = other.Lsn;
    Range = other.Range;
    Mask = other.Mask;
    LockRange = other.LockRange;
    Armed = other.Armed;

    other.Armed = false;
    other.LockableRanges = nullptr;
    return *this;
}

void TRangeLock::Arm()
{
    if (Armed) {
        return;
    }

    Armed = true;

    if (Lsn) {
        LockableRanges->LockPBuffer(Lsn);
    } else {
        Y_ABORT_UNLESS(!Mask.Empty());
        LockRange = LockableRanges->LockDDiskRange(Range, Mask);
    }
}

TRangeLock::TRangeLock(ILockableRanges* lockableRanges, ui64 lsn)
    : LockableRanges(lockableRanges)
    , Lsn(lsn)
{}

TRangeLock::TRangeLock(
    ILockableRanges* lockableRanges,
    TBlockRange64 range,
    THostMask mask)
    : LockableRanges(lockableRanges)
    , Range(range)
    , Mask(mask)
{}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
