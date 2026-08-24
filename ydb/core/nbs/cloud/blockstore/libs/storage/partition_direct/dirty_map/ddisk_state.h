#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_field.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/count_size.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <util/generic/string.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Allows to receive notifications about changes in data that need to be
// persisted in the partition local database.
struct IBehindAheadMonitor
{
    virtual ~IBehindAheadMonitor() = default;

    virtual void OnBehindAheadChanged() = 0;
};

class TDDiskState
{
public:
    enum class EState
    {
        Disabled,   // There are no DDisks with data on the host and DDisk
                    // cannot be used.

        Operational,   // The DDisk is fully functional and can be read from
                       // anywhere. BehindField and AheadField are empty.

        Fresh,   // The ddisk is only partially filled, and you can only read
                 // from the blocks below the OperationalBlockCount.
                 // The AheadField shows which ranges flushed over watermark and
                 // can be read from. The BehindField shows which ranges
                 // outdated and can't be read.
    };

    enum class EFlushCompletion
    {
        Completed,   // Data flushed to DDisk
        Missed,      // Data not flushed to DDisk
    };

    // Enables the use of DDisk. If the operational blocks count less then total
    // block count, then the DDisk is only partially filled (fresh).
    void Init(
        IBehindAheadMonitor* behindAheadMonitor,
        ui64 totalBlockCount,
        ui64 operationalBlockCount);

    // Save ahead and behind maps to proto.
    void Save(TDDiskStateProto* proto) const;
    // Load ahead and behind maps from proto.
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
    // Updates the BehindField and the Ahead Field if required.
    void OnRangeFlushed(TBlockRange64 range, EFlushCompletion flush);

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange64 range) const;
    [[nodiscard]] bool HasBehindOverlapping(TBlockRange64 range) const;

    [[nodiscard]] std::optional<TBlockRange64> GetFreshRange() const;
    void RangeSynced(TBlockRange64 range);

    [[nodiscard]] TCountAndSize GetAheadSegmentsStat() const;
    [[nodiscard]] TCountAndSize GetBehindSegmentsStat() const;

    void UpdateWatermarkDebugOnly(ui64 blockCount);
    [[nodiscard]] TString DebugPrint() const;
    [[nodiscard]] TString DebugPrintAhead() const;
    [[nodiscard]] TString DebugPrintBehind() const;
    [[nodiscard]] TString DebugPrintAheadBehindBrief() const;

private:
    [[nodiscard]] bool IsFresh() const;
    void UpdateState(bool force);
    void AddAhead(TBlockRange64 range);
    void AddBehind(TBlockRange64 range);

    IBehindAheadMonitor* BehindAheadMonitor = nullptr;

    EState State = EState::Disabled;

    ui64 TotalBlockCount = 0;

    // If the block address below OperationalBlockCount, then it can be read
    // from DDisk (except BehindField).
    ui64 OperationalBlockCount = 0;

    // Lagging means that flush operations are not performed and DDisk has
    // outdated data in the ranges listed in the BehindField.
    bool Lagging = false;
    TBlockRangeField BehindField;
    // When a user writes to a range above OperationalBlockCount, this range has
    // up-to-date data and does not require sync.
    TBlockRangeField AheadField;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
