#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/record_id.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct ILockableRanges
{
    using TLockRangeHandle = ui64;

    virtual ~ILockableRanges() = default;

    virtual void LockPBuffer(TRecordId recordId) = 0;
    virtual void UnlockPBuffer(TRecordId recordId) = 0;
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
    void Disarm();

private:
    friend class TBlocksDirtyMap;
    friend class TRangeLockAccess;
    friend class TDDiskDataCopier;

    // Lock the PBuffer record with the given id.
    TRangeLock(ILockableRanges* lockableRanges, TRecordId recordId);

    // Lock the range on the DDisks specified by the mask.
    TRangeLock(
        ILockableRanges* lockableRanges,
        TBlockRange64 range,
        THostMask mask);

    ILockableRanges* LockableRanges = nullptr;
    TRecordId RecordId;
    TBlockRange64 Range;
    THostMask Mask;

    ILockableRanges::TLockRangeHandle LockRange{};
    bool Armed = false;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
