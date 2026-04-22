#include "range_locker.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock::TRangeLock(TRangeLock&& other) noexcept
    : LockableRanges(other.LockableRanges)
    , Lsn(other.Lsn)
    , Range(other.Range)
    , LockRange(other.LockRange)
    , Armed(other.Armed)
{
    other.Armed = false;
    other.LockRange = {};
}

TRangeLock::~TRangeLock()
{
    if (!Armed) {
        return;
    }

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
    Armed = other.Armed;
    LockRange = other.LockRange;
    other.Armed = false;
    other.LockRange = {};
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
    TLocationMask mask)
    : LockableRanges(lockableRanges)
    , Range(range)
    , Mask(mask)
{}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
