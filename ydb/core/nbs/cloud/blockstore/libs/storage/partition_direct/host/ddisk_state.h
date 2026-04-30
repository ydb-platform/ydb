#pragma once

#include "host_mask.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TDDiskState
{
public:
    enum class EState
    {
        Operational,   // The ddisk is fully functional and can be read from
                       // anywhere.
        Fresh,   // The ddisk is only partially filled, and you can only read
                 // from the blocks below the OperationalBlockCount.
    };

    void Init(ui64 totalBlockCount, ui64 operationalBlockCount);

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange64 range) const;
    [[nodiscard]] bool NeedFlushToDDisk(TBlockRange64 range) const;

    void SetReadWatermark(ui64 blockCount);
    void SetFlushWatermark(ui64 blockCount);
    [[nodiscard]] ui64 GetOperationalBlockCount() const;

    [[nodiscard]] TString DebugPrint() const;

private:
    void UpdateState();

    EState State = EState::Operational;

    ui64 TotalBlockCount = 0;

    // If the block address below OperationalBlockCount, then it can be read
    // from DDisk.
    ui64 OperationalBlockCount = 0;

    // If the block address below FlushableBlockCount, then it should be written
    // (flushed) to DDisk.
    ui64 FlushableBlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDDiskStateList
{
public:
    TDDiskStateList() = default;
    TDDiskStateList(size_t hostCount, ui32 blockSize, ui64 totalBlockCount);

    [[nodiscard]] size_t HostCount() const;

    void MarkFresh(THostIndex h, ui64 bytesOffset);
    void SetReadWatermark(THostIndex h, ui64 bytesOffset);
    void SetFlushWatermark(THostIndex h, ui64 bytesOffset);

    // Returns the offset (bytes) up to which the ddisk holds data.
    // nullopt means the ddisk is fully Operational.
    [[nodiscard]] std::optional<ui64> GetFreshWatermark(THostIndex h) const;

    [[nodiscard]] bool CanReadFromDDisk(
        THostIndex h,
        TBlockRange64 range) const;
    [[nodiscard]] bool NeedFlushToDDisk(
        THostIndex h,
        TBlockRange64 range) const;

    // Returns the subset of `mask` whose ddisks can serve a read for `range`.
    [[nodiscard]] THostMask FilterReadable(
        THostMask mask,
        TBlockRange64 range) const;

    [[nodiscard]] TString DebugPrint() const;

private:
    ui32 BlockSize = 0;
    TVector<TDDiskState> States;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
