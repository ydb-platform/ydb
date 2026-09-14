#include "range_locker_access.h"

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRangeLock TRangeLockAccess::Make(
    ILockableRangesWeakPtr lockableRanges,
    TPBufferKey pBufferKey)
{
    return TRangeLock(std::move(lockableRanges), pBufferKey);
}

TRangeLock TRangeLockAccess::Make(
    ILockableRangesWeakPtr lockableRanges,
    TBlockRange64 range,
    THostMask mask)
{
    return TRangeLock(std::move(lockableRanges), range, mask);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
