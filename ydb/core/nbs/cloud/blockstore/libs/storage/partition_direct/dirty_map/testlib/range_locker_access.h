#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/range_locker.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRangeLockAccess
{
public:
    static TRangeLock Make(
        ILockableRangesWeakPtr lockableRanges,
        TPBufferKey pBufferKey);

    static TRangeLock Make(
        ILockableRangesWeakPtr lockableRanges,
        TBlockRange64 range,
        THostMask mask);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
