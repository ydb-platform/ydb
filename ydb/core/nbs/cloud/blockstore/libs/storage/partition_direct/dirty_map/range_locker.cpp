#include "range_locker.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock::TRangeLock(TRangeLock&& other) noexcept
    : LockableRanges(other.LockableRanges)
    , RecordId(other.RecordId)
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
    Disarm();
}

TRangeLock& TRangeLock::operator=(TRangeLock&& other) noexcept
{
    LockableRanges = other.LockableRanges;
    RecordId = other.RecordId;
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

    if (RecordId.Lsn) {
        LockableRanges->LockPBuffer(RecordId);
    } else {
        Y_ABORT_UNLESS(!Mask.Empty());
        LockRange = LockableRanges->LockDDiskRange(Range, Mask);
    }
}

void TRangeLock::Disarm()
{
    if (!Armed) {
        return;
    }
    Armed = false;

    Y_ABORT_UNLESS(LockableRanges);

    if (RecordId.Lsn) {
        LockableRanges->UnlockPBuffer(RecordId);
    } else {
        LockableRanges->UnLockDDiskRange(LockRange);
    }
}

TRangeLock::TRangeLock(ILockableRanges* lockableRanges, TRecordId recordId)
    : LockableRanges(lockableRanges)
    , RecordId(recordId)
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
