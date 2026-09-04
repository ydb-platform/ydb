#include "range_locker.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock::TRangeLock(TRangeLock&& other) noexcept
    : LockableRanges(std::move(other.LockableRanges))
    , PBufferKey(other.PBufferKey)
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
    PBufferKey = other.PBufferKey;
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
        if (PBufferKey.Lsn) {
            lockableRanges->LockPBuffer(PBufferKey);
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
        if (PBufferKey.Lsn) {
            lockableRanges->UnlockPBuffer(PBufferKey);
        } else {
            lockableRanges->UnLockDDiskRange(LockRange);
        }
    }
}

TRangeLock::TRangeLock(
    ILockableRangesWeakPtr lockableRanges,
    TPBufferKey pBufferKey)
    : LockableRanges(std::move(lockableRanges))
    , PBufferKey(pBufferKey)
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
