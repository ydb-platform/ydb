#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

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

    // Lock PBuffer with given lsn.
    TRangeLock(ILockableRanges* lockableRanges, ui64 lsn);

    // Lock the range on the DDisks specified by the mask.
    TRangeLock(
        ILockableRanges* lockableRanges,
        TBlockRange64 range,
        THostMask mask);

    ILockableRanges* LockableRanges = nullptr;
    ui64 Lsn = 0;
    TBlockRange64 Range;
    THostMask Mask;

    ILockableRanges::TLockRangeHandle LockRange{};
    bool Armed = false;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
