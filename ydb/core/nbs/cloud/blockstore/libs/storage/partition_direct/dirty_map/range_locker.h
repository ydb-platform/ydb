#pragma once

#include "host_status.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct ILockableRanges
{
    using TLockRangeHandle = ui64;

    virtual ~ILockableRanges() = default;

    virtual void LockPBuffer(ui64 lsn) = 0;
    virtual void UnlockPBuffer(ui64 lsn) = 0;
    virtual TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        THostMask mask) = 0;
    virtual void UnLockDDiskRange(TLockRangeHandle handle) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRangeLock: TDisableCopy
{
public:
    TRangeLock(TRangeLock&& other) noexcept;
    ~TRangeLock();

    TRangeLock& operator=(TRangeLock&& other) noexcept;

    void Arm();

private:
    friend class TBlocksDirtyMap;
    friend class TRangeLockAccess;
    friend class TDDiskDataCopier;

    TRangeLock(ILockableRanges* lockableRanges, ui64 lsn);
    TRangeLock(
        ILockableRanges* lockableRanges,
        TBlockRange64 range,
        THostMask mask);

    ILockableRanges* LockableRanges;
    ui64 Lsn = 0;
    TBlockRange64 Range;
    THostMask Mask;

    ILockableRanges::TLockRangeHandle LockRange{};
    bool Armed = false;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
