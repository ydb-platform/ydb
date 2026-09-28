#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range_field.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <util/generic/string.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Allows to receive notifications about changes in data that need to be
// persisted in the partition local database.
struct IBehindMonitor
{
    virtual ~IBehindMonitor() = default;

    virtual void OnBehindChanged() = 0;
};

// Tracks the synchronization state of one DDisk.
//
// BehindField contains ranges that do not have up-to-date data. Only the
// continuous prefix before its first range can be read. An empty BehindField
// means that the whole DDisk is operational.
//
// While the DDisk is lagging, successful synchronization callbacks are stale
// and must be ignored: the corresponding range may have been dirtied again
// after the synchronization started. Once lagging stops, the range can be
// synchronized and removed from BehindField.
class TDDiskState
{
public:
    enum class EState
    {
        Disabled,   // There are no DDisks with data on the host and DDisk
                    // cannot be used.

        Operational,   // The DDisk is fully functional and can be read from
                       // anywhere. BehindField is empty.

        Fresh,   // The DDisk is only partially filled. Only the continuous
                 // prefix before the first BehindField range can be read.
    };

    enum class EFlushCompletion
    {
        Completed,   // Data flushed to DDisk
        Missed,      // Data not flushed to DDisk
    };

    // Creates DDisk state with the specified allocator and maximum block count.
    TDDiskState(IArenaAllocatorPtr arenaAllocator, ui16 maxBlockCount);

    // Enables the use of DDisk. If the operational blocks count less then total
    // block count, then the DDisk is only partially filled (fresh).
    void Init(
        IBehindMonitor* behindMonitor,
        ui16 totalBlockCount,
        ui16 operationalBlockCount);

    // Save the behind map to proto.
    void Save(TDDiskStateProto* proto) const;
    // Load the behind map from proto.
    void Load(const TDDiskStateProto& proto);

    // Completely disables DDisk usage.
    void SwitchOffline();

    [[nodiscard]] bool IsLagging() const;
    // DDisk has stopped receiving writes. Now the written ranges are
    // interpreted as "bad" and added to the BehindField.
    void StartLagging();
    // DDisk now receive all writes. The written ranges are interpreted as
    // "good" and removed from the BehindField.
    void StopLagging();
    // Is it necessary to receive information about all written ranges. If true
    // is returned, it means that all ranged that have been flushed must be
    // passed to the OnRangeFlushed() method.
    [[nodiscard]] bool IsTrackingEnabled() const;
    // Updates the BehindField if required.
    void OnRangeFlushed(TBlockRange16 range, EFlushCompletion flush);

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange16 range) const;
    [[nodiscard]] bool HasBehindOverlapping(TBlockRange16 range) const;

    [[nodiscard]] std::optional<TBlockRange16> GetFreshRange() const;
    void RangeSynced(TBlockRange16 range);

    // Returns the number of up-to-date blocks while the disk is not lagging.
    [[nodiscard]] ui16 GetFreshBlockCount() const;
    // Returns the number of outdated blocks while the disk is lagging.
    [[nodiscard]] ui16 GetRottenBlockCount() const;

    // Memory usage.
    [[nodiscard]] TArenaPoolStats GetMemoryStats() const;

    void SetReadablePrefixDebugOnly(ui16 readableBlockCount);
    [[nodiscard]] TString DebugPrint() const;
    [[nodiscard]] TString DebugPrintBehind() const;
    [[nodiscard]] TString DebugPrintBehindBrief() const;

private:
    void CheckInvariants() const;
    [[nodiscard]] bool IsFresh() const;
    [[nodiscard]] ui16 GetReadableBlockCount() const;
    void UpdateState(bool force);
    void RemoveBehind(TBlockRange16 range);
    void AddBehind(TBlockRange16 range);

    IBehindMonitor* BehindMonitor = nullptr;

    const IArenaAllocatorPtr ArenaAllocator;

    EState State = EState::Disabled;

    ui16 TotalBlockCount = 0;

    // Lagging means that flush operations are not performed and DDisk has
    // outdated data in the ranges listed in the BehindField.
    bool Lagging = false;
    TBlockRangeField BehindField;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
