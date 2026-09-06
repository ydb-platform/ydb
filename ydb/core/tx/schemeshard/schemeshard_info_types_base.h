#pragma once

#include "schemeshard_info_types_fwd.h"
#include "schemeshard_datashard_fwd.h"
#include "schemeshard_identificators.h"
#include "schemeshard_info_types_helpers.h"
#include "schemeshard_path_element.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_types.h"

#include <util/generic/yexception.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/tx/message_seqno.h>

#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr {

namespace NScheme {
class TTypeRegistry;
}

bool PartitionConfigHasExternalBlobsEnabled(const NKikimrSchemeOp::TPartitionConfig& partitionConfig);

namespace NSchemeShard {

class TSchemeShard;

struct TForceShardSplitSettings {
    ui64 ForceShardSplitDataSize;
    bool DisableForceShardSplit;
};

struct TSplitSettings {
    TControlWrapper SplitMergePartCountLimit;
    TControlWrapper FastSplitSizeThreshold;
    TControlWrapper FastSplitRowCountThreshold;
    TControlWrapper FastSplitCpuPercentageThreshold;
    TControlWrapper SplitByLoadEnabled;
    TControlWrapper SplitByLoadMaxShardsDefault;
    TControlWrapper MergeByLoadMinUptimeSec;
    TControlWrapper MergeByLoadMinLowLoadDurationSec;
    TControlWrapper ForceShardSplitDataSize;
    TControlWrapper DisableForceShardSplit;

    TSplitSettings()
        : SplitMergePartCountLimit(2000, -1, 1000000)
        , FastSplitSizeThreshold(4*1000*1000, 100*1000, 4ll*1000*1000*1000)
        , FastSplitRowCountThreshold(100*1000, 1000, 1ll*1000*1000*1000)
        , FastSplitCpuPercentageThreshold(50, 1, 146)
        , SplitByLoadEnabled(1, 0, 1)
        , SplitByLoadMaxShardsDefault(50, 0, 10000)
        , MergeByLoadMinUptimeSec(10*60, 0, 4ll*1000*1000*1000)
        , MergeByLoadMinLowLoadDurationSec(1*60*60, 0, 4ll*1000*1000*1000)
        , ForceShardSplitDataSize(2ULL * 1024 * 1024 * 1024, 10 * 1024 * 1024, 16ULL * 1024 * 1024 * 1024)
        , DisableForceShardSplit(0, 0, 1)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(SplitMergePartCountLimit,         icb->SchemeShardControls.SplitMergePartCountLimit);
        TControlBoard::RegisterSharedControl(FastSplitSizeThreshold,           icb->SchemeShardControls.FastSplitSizeThreshold);
        TControlBoard::RegisterSharedControl(FastSplitRowCountThreshold,       icb->SchemeShardControls.FastSplitRowCountThreshold);
        TControlBoard::RegisterSharedControl(FastSplitCpuPercentageThreshold,  icb->SchemeShardControls.FastSplitCpuPercentageThreshold);

        TControlBoard::RegisterSharedControl(SplitByLoadEnabled,               icb->SchemeShardControls.SplitByLoadEnabled);
        TControlBoard::RegisterSharedControl(SplitByLoadMaxShardsDefault,      icb->SchemeShardControls.SplitByLoadMaxShardsDefault);
        TControlBoard::RegisterSharedControl(MergeByLoadMinUptimeSec,          icb->SchemeShardControls.MergeByLoadMinUptimeSec);
        TControlBoard::RegisterSharedControl(MergeByLoadMinLowLoadDurationSec, icb->SchemeShardControls.MergeByLoadMinLowLoadDurationSec);

        TControlBoard::RegisterSharedControl(ForceShardSplitDataSize,          icb->SchemeShardControls.ForceShardSplitDataSize);
        TControlBoard::RegisterSharedControl(DisableForceShardSplit,           icb->SchemeShardControls.DisableForceShardSplit);
    }

    TForceShardSplitSettings GetForceShardSplitSettings() const {
        return TForceShardSplitSettings{
            .ForceShardSplitDataSize = ui64(ForceShardSplitDataSize),
            .DisableForceShardSplit = ui64(DisableForceShardSplit) != 0,
        };
    }
};

struct TBackupToS3Settings {
    // Async Replication
    TControlWrapper EnableAsyncReplicationExport;
    TControlWrapper EnableAsyncReplicationImport;
    // Transfer
    TControlWrapper EnableTransferExport;
    TControlWrapper EnableTransferImport;
    // External Data Source
    TControlWrapper EnableExternalDataSourceExport;
    TControlWrapper EnableExternalDataSourceImport;
    // External Table
    TControlWrapper EnableExternalTableExport;
    TControlWrapper EnableExternalTableImport;
    // System Views
    TControlWrapper EnableSysViewPermissionsExport;
    TControlWrapper EnableSysViewPermissionsImport;

    TBackupToS3Settings()
        : EnableAsyncReplicationExport(1, 0, 1)
        , EnableAsyncReplicationImport(1, 0, 1)
        , EnableTransferExport(1, 0, 1)
        , EnableTransferImport(1, 0, 1)
        , EnableExternalDataSourceExport(1, 0, 1)
        , EnableExternalDataSourceImport(1, 0, 1)
        , EnableExternalTableExport(1, 0, 1)
        , EnableExternalTableImport(1, 0, 1)
        , EnableSysViewPermissionsExport(1, 0, 1)
        , EnableSysViewPermissionsImport(1, 0, 1)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(EnableAsyncReplicationExport, icb->BackupControls.S3Controls.EnableAsyncReplicationExport);
        TControlBoard::RegisterSharedControl(EnableAsyncReplicationImport, icb->BackupControls.S3Controls.EnableAsyncReplicationImport);

        TControlBoard::RegisterSharedControl(EnableTransferExport, icb->BackupControls.S3Controls.EnableTransferExport);
        TControlBoard::RegisterSharedControl(EnableTransferImport, icb->BackupControls.S3Controls.EnableTransferImport);

        TControlBoard::RegisterSharedControl(EnableExternalDataSourceExport, icb->BackupControls.S3Controls.EnableExternalDataSourceExport);
        TControlBoard::RegisterSharedControl(EnableExternalDataSourceImport, icb->BackupControls.S3Controls.EnableExternalDataSourceImport);

        TControlBoard::RegisterSharedControl(EnableExternalTableExport, icb->BackupControls.S3Controls.EnableExternalTableExport);
        TControlBoard::RegisterSharedControl(EnableExternalTableImport, icb->BackupControls.S3Controls.EnableExternalTableImport);

        TControlBoard::RegisterSharedControl(EnableSysViewPermissionsExport, icb->BackupControls.S3Controls.EnableSysViewPermissionsExport);
        TControlBoard::RegisterSharedControl(EnableSysViewPermissionsImport, icb->BackupControls.S3Controls.EnableSysViewPermissionsImport);
    }
};

struct TBackupSettings {
    TBackupToS3Settings S3Settings;

    TBackupSettings() = default;

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        S3Settings.Register(icb);
    }
};

// Per-incremental restore orchestrator rate-limit settings. Sentinel -1 means unbounded.
struct TIncrementalRestoreSettings {
    TControlWrapper MaxIncrementalRestoreTablesInFlight;
    // Wall-clock deadlines (seconds). Sentinel -1 disables the deadline.
    TControlWrapper MaxIncrementalRestoreOverallDurationSeconds;
    TControlWrapper MaxIncrementalRestoreStageDurationSeconds;

    TIncrementalRestoreSettings()
        : MaxIncrementalRestoreTablesInFlight(32, -1, 1000000)
        , MaxIncrementalRestoreOverallDurationSeconds(86400, -1, 604800)
        , MaxIncrementalRestoreStageDurationSeconds(86400, -1, 604800)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreTablesInFlight,
                                             icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreOverallDurationSeconds,
                                             icb->SchemeShardControls.MaxIncrementalRestoreOverallDurationSeconds);
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreStageDurationSeconds,
                                             icb->SchemeShardControls.MaxIncrementalRestoreStageDurationSeconds);
    }
};


struct TBindingsRoomsChange {
    TChannelsBindings ChannelsBindings;
    NKikimrSchemeOp::TPartitionConfig PerShardConfig;
    bool ChannelsBindingsUpdated = false;
};

/**
 * Maps original channels bindings to possible updates
 */
TChannelsMapping GetPoolsMapping(const TChannelsBindings& bindings);


struct TTableShardInfo {
    TShardIdx ShardIdx = InvalidShardIdx;
    TString EndOfRange;
    TInstant LastCondErase;
    TInstant NextCondErase;
    mutable TMaybe<TDuration> LastCondEraseLag;
    ui64 Position = 0;  // index in TTableInfo::Partitions; maintained by TTableInfo, not persisted

    // TODO: remove this ctor. It's used for vector.resize() that is not clear.
    TTableShardInfo() = default;

    TTableShardInfo(const TShardIdx& idx, TString rangeEnd, ui64 lastCondErase = 0, ui64 nextCondErase = 0)
        : ShardIdx(idx)
        , EndOfRange(rangeEnd)
        , LastCondErase(TInstant::FromValue(lastCondErase))
        , NextCondErase(TInstant::FromValue(nextCondErase))
    {}
};

struct TColumnFamiliesMerger {
    TColumnFamiliesMerger(NKikimrSchemeOp::TPartitionConfig &container);

    bool Has(ui32 familyId) const;
    NKikimrSchemeOp::TFamilyDescription* Get(ui32 familyId, TString &errDescr);
    NKikimrSchemeOp::TFamilyDescription* AddOrGet(ui32 familyId, TString& errDescr);
    NKikimrSchemeOp::TFamilyDescription* Get(const TString& familyName, TString& errDescr);
    NKikimrSchemeOp::TFamilyDescription* AddOrGet(const TString& familyName, TString& errDescr);
    NKikimrSchemeOp::TFamilyDescription* Get(ui32 familyId, const TString& familyName, TString& errDescr);
    NKikimrSchemeOp::TFamilyDescription* AddOrGet(ui32 familyId, const TString&  familyName, TString& errDescr);

private:
    static constexpr ui32 MAX_AUTOGENERATED_FAMILY_ID = Max<ui32>() - 1;

    static const TString& CanonizeName(const TString& familyName);

    NKikimrSchemeOp::TPartitionConfig &Container;
    THashMap<ui32, size_t> DeduplicationById;
    THashMap<ui32, TString> NameByIds;
    THashMap<TString, ui32> IdByName;
    ui32 NextAutogenId = 0;
};

struct TPartitionConfigMerger {
    static constexpr ui32 MaxFollowersCount = 3;

    static NKikimrSchemeOp::TPartitionConfig DefaultConfig(const TAppData* appData, const std::optional<TString>& defaultPoolKind);
    static bool ApplyChanges(
        NKikimrSchemeOp::TPartitionConfig& result,
        const NKikimrSchemeOp::TPartitionConfig& src, const NKikimrSchemeOp::TPartitionConfig& changes,
        const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription>& columns,
        const TAppData* appData, const bool isServerlessDomain, TString& errDescr);

    static bool ApplyChangesInColumnFamilies(
        NKikimrSchemeOp::TPartitionConfig& result,
        const NKikimrSchemeOp::TPartitionConfig& src, const NKikimrSchemeOp::TPartitionConfig& changes,
        const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription>& columns,
        const bool isServerlessDomain, TString& errDescr);

    static THashMap<ui32, size_t> DeduplicateColumnFamiliesById(NKikimrSchemeOp::TPartitionConfig& config);
    static THashMap<ui32, size_t> DeduplicateStorageRoomsById(NKikimrSchemeOp::TPartitionConfig& config);
    static NKikimrSchemeOp::TFamilyDescription& MutableColumnFamilyById(
        NKikimrSchemeOp::TPartitionConfig& partitionConfig,
        THashMap<ui32, size_t>& posById,
        ui32 familyId);

    static bool VerifyCreateParams(
        const NKikimrSchemeOp::TPartitionConfig& config,
        const TAppData* appData, const bool shadowDataAllowed, TString& errDescr);

    static bool VerifyAlterParams(
        const NKikimrSchemeOp::TPartitionConfig& srcConfig,
        const NKikimrSchemeOp::TPartitionConfig& dstConfig,
        const TAppData* appData,
        const bool shadowDataAllowed,
        TString& errDescr
        );

    static bool VerifyCompactionPolicy(
        const NKikimrCompaction::TCompactionPolicy& policy,
        TString& err);

    static bool VerifyCommandOnFrozenTable(
        const NKikimrSchemeOp::TPartitionConfig& srcConfig,
        const NKikimrSchemeOp::TPartitionConfig& dstConfig);

};

struct TPartitionStats {
    /**
     * The container for the latest time stamps when the CPU usage exceeded
     * specific thresholds: 2%, 5%, 10%, 20%, 30%.
     */
    struct TTopCpuUsage {
        /**
         * Describes the boundaries for a CPU usage bucket.
         */
        struct TBucket {
            /**
             * The low boundary for this bucket.
             *
             * @note If the current CPU usage exceeds this value, this bucket is updated.
             */
            const ui32 LowBoundary;

            /**
             * The effective CPU usage value for this bucket.
             *
             * @note If this bucket falls within the given time period,
             *       this value is used as the assumed CPU usage percentage.
             */
            const ui32 EffectiveValue;
        };

        /**
         * The boundaries for all CPU usage buckets tracked by this class.
         *
         * @warning This list must be sorted by the threshold value (in the ascending order).
         */
        static constexpr std::array<TBucket, 5> Buckets = {{
            {2, 5},   // >=  2% -->  5% CPU usage
            {5, 10},  // >=  5% --> 10% CPU usage
            {10, 20}, // >= 10% --> 20% CPU usage
            {20, 30}, // >= 20% --> 30% CPU usage
            {30, 40}, // >= 30% --> 40% CPU usage
        }};

        /**
         * The time when each usage bucket was updated.
         */
        std::array<TInstant, Buckets.size()> BucketUpdateTimes;

        /**
         * Update the CPU usage data using values from another container.
         *
         * @param[in] usage The container to update the usage data from
         */
        void Update(const TTopCpuUsage& usage) {
            // Keep only the latest time for each bucket
            for (ui64 i = 0; i < Buckets.size(); ++i) {
                BucketUpdateTimes[i] = std::max(BucketUpdateTimes[i], usage.BucketUpdateTimes[i]);
            }
        }

        /**
         * Update the historical CPU usage.
         *
         * @param[in] rawCpuUsage The current CPU usage
         * @param[in] now The current time
         */
        void UpdateCpuUsage(ui64 rawCpuUsage, TInstant now) {
            ui32 percent = static_cast<ui32>(rawCpuUsage * 0.000001 * 100);

            // Update all buckets, which have low boundaries below the given CPU usage
            for (ui64 i = 0; i < Buckets.size(); ++i) {
                if (percent < Buckets[i].LowBoundary) {
                    return;
                }

                BucketUpdateTimes[i] = now;
            }
        }

        /**
         * Get the peak CPU usage percentage that has been observed since the given time.
         *
         * @note This function does not return the actual peak CPU usage value.
         *       The return value is one of the preset thresholds, which this class
         *       tracks (2%, 5%, 10%, 20%, 30% and 40%).
         *
         * @todo Fix the case when stats were not collected yet
         *
         * @param[in] since The time from which to calculate the peak CPU usage
         *
         * @return The peak CPU usage (as a percentage) since the given time
         */
        ui32 GetLatestMaxCpuUsagePercent(TInstant since) const {
            // Find the highest bucket (from the end of the list),
            // which was updated after the given time
            for (i64 i = Buckets.size() - 1; i >= 0; --i) {
                if (BucketUpdateTimes[i] > since) {
                    return Buckets[i].EffectiveValue;
                }
            }

            // No bucket was found, return at least some minimum CPU usage percentage
            return 2;
        }
    };

    TMessageSeqNo SeqNo;

    ui64 RowCount = 0;
    ui64 DataSize = 0;
    ui64 IndexSize = 0;
    ui64 ByKeyFilterSize = 0;

    ui64 SmallBlobsVolumeBytes = 0;
    ui64 SmallBlobsCount = 0;

    struct TStoragePoolStats {
        ui64 DataSize = 0;
        ui64 IndexSize = 0;
    };
    THashMap<TString, TStoragePoolStats> StoragePoolsStats;

    TInstant LastAccessTime;
    TInstant LastUpdateTime;
    TDuration TxCompleteLag;

    ui64 ImmediateTxCompleted = 0;
    ui64 PlannedTxCompleted = 0;
    ui64 TxRejectedByOverload = 0;
    ui64 TxRejectedBySpace = 0;
    ui64 InFlightTxCount = 0;

    ui64 RowUpdates = 0;
    ui64 RowDeletes = 0;
    ui64 RowReads = 0;
    ui64 RangeReads = 0;
    ui64 RangeReadRows = 0;

    ui64 Memory = 0;
    ui64 Network = 0;
    ui64 Storage = 0;
    ui64 ReadThroughput = 0;
    ui64 WriteThroughput = 0;
    ui64 ReadIops = 0;
    ui64 WriteIops = 0;

    THashSet<TTabletId> PartOwners;
    ui64 PartCount = 0;
    ui64 SearchHeight = 0;
    ui64 FullCompactionTs = 0;
    ui64 MemDataSize = 0;
    // NKikimrTxDataShard::Unknown. Keep the wire value here so this common
    // in-memory types header does not pull in the full DataShard protobuf.
    ui32 ShardState = 0xFFFF;

    ui64 LocksAcquired = 0;
    ui64 LocksWholeShard = 0;
    ui64 LocksBroken = 0;

    // True when PartOwners has parts from other tablets
    bool HasBorrowedData = false;

    // True when lent parts to other tablets
    bool HasLoanedData = false;

    bool HasSchemaChanges = false;

    // Tablet actor started at
    TInstant StartTime;

    /**
     * The CPU usage percentage statistics represented as a time series:
     * the last time the CPU usage exceeded 30%, the last time the CPU usage
     * exceeded 20% and so on.
     *
     * @warning This is a combined statistics, which includes both the leader
     *          and all the followers for the given partition. The CPU usage is treated
     *          as a maximum across all followers and the leader, not as a sum.
     *          For example, the data bucket for the 30% contains the last time,
     *          when the CPU usage exceeded 30% for any follower or the leader.
     *
     * @note This field is used to control the merge-by-load operations.
     *       It is not used for the split-by-load operations or for any other purposes.
     *
     * @note Why does this work for the merge-by-load operation?
     *
     *       When the SchemeShard actor received the EvPeriodicTableStats message
     *       from one of the followers (or the leader) for the given partition,
     *       it updates this field and then figures out the maximum CPU load
     *       percentage that was used by the given partition over a preconfigured
     *       time interval into the past (1 hour by default). If this maximum
     *       CPU usage percentage does not exceed a certain preconfigured threshold
     *       (70% of the split-by-load threshold), then this partition becomes
     *       the anchor for the merge-by-load operation.
     *
     *       Once the anchor is picked, the code tries to add to the given partition
     *       as many partitions to the left and to the right of it as possible.
     *       When adding a potential candidate to the merge set, the code takes
     *       the maximum CPU usage percentage for the given potential candidate
     *       over the same time interval into the past and adds it to the combined
     *       CPU usage percentage. The code continues adding partitions
     *       to the merge set as long as the combined CPU usage percentage for
     *       all partitions in the merge set stays below the same preconfigured
     *       threshold (70% of the split-by-load threshold).
     *
     *       Once all possible partitions have been added to the merge set
     *       (and this set contains more than one partition), the entire set
     *       is merged into a single partition.
     *
     *       Notice that both picking the anchor partition for the merge-by-load
     *       operation and adding a partition to the merge set requires that
     *       the observed CPU load for the given partition stays below a certain level
     *       for all followers and the leader for the given partition.
     *       Keeping the maximum CPU usage percentage across all followers
     *       and the leader is sufficient to verify this requirement.
     */
    TTopCpuUsage TopCpuUsage;

    void SetCurrentRawCpuUsage(ui64 rawCpuUsage, TInstant now) {
        CPU = rawCpuUsage;
        TopCpuUsage.UpdateCpuUsage(rawCpuUsage, now);
    }

    ui64 GetCurrentRawCpuUsage() const {
        return CPU;
    }

    ui32 GetLatestMaxCpuUsagePercent(TInstant since) const {
        return TopCpuUsage.GetLatestMaxCpuUsagePercent(since);
    }

private:
    /**
     * The last observed CPU usage for the given partition.
     *
     * @warning This value is updated only by the data received from the leader.
     *          Unlike the TopCpuUsage field, it does not include any followers.
     */
    ui64 CPU = 0;
};

struct TStoragePoolStatsDelta {
    i64 DataSize = 0;
    i64 IndexSize = 0;
};

NProtoBuf::Timestamp SecondsToProtoTimeStamp(ui64 sec);

std::optional<std::pair<i64, i64>> ValidateSequenceType(
    const TString& sequenceName,
    const TString& dataType,
    const NScheme::TTypeRegistry& typeRegistry,
    bool pgTypesEnabled,
    TString& errStr);

using TDiskSpaceUsageDelta = TVector<std::pair<TString, TStoragePoolStatsDelta>>;

struct TTableAggregatedStats {
    TPartitionStats Aggregated;
    THashMap<TShardIdx, TPartitionStats> PartitionStats;
    size_t PartitionStatsUpdated = 0;

    THashSet<TShardIdx> UpdatedStats;

    bool AreStatsFull() const {
        return Aggregated.PartCount && UpdatedStats.size() == Aggregated.PartCount;
    }

    void UpdateShardStats(TDiskSpaceUsageDelta* diskSpaceUsageDelta, TShardIdx datashardIdx, const TPartitionStats& newStats, TInstant now);

    /**
     * Update the statistics data for the given shard and the given follower
     * using the data from the EvPeriodicTableStats message.
     *
     * @param[in] followerId The follower ID
     * @param[in] shardIdx The shard index
     * @param[in] newStats The new statistics to use for updating
     */
    void UpdateShardStatsForFollower(
        ui64 followerId,
        const TShardIdx& shardIdx,
        const TPartitionStats& newStats
    );

    // Subtracts current-state metrics of each shard from Aggregated
    // and removes each shard from PartitionStats/UpdatedStats.
    // Silently skips shards not in PartitionStats.
    void RemoveShardStats(const TVector<TShardIdx>& keys, TInstant now);
};

struct TAggregatedStats : public TTableAggregatedStats {
    THashMap<TPathId, TTableAggregatedStats> TableStats;

    void UpdateTableStats(TShardIdx datashardIdx, const TPathId& pathId, const TPartitionStats& newStats, TInstant now);
};

struct TSubDomainInfo;

// Indexed min-heap keyed by TShardIdx.
// Pos map enables O(log N) targeted Erase (O(1) position lookup + O(log N) sift).
class TCondEraseSchedule {
public:
    using TEntry = std::pair<TInstant, TShardIdx>;

    void Push(TInstant t, const TShardIdx& shardIdx) {
        Y_ABORT_UNLESS(!Contains(shardIdx));
        const ui32 i = Heap.size();
        Heap.push_back({t, shardIdx});
        Pos[shardIdx] = i;
        SiftUp(i);
    }

    // Batch load: call PushRaw for each entry, then BuildHeap once — O(N) total.
    void PushRaw(TInstant t, const TShardIdx& shardIdx) {
        Heap.push_back({t, shardIdx});
    }
    void BuildHeap() {
        std::make_heap(Heap.begin(), Heap.end(), TGreater{});
        Pos.clear();
        Pos.reserve(Heap.size());
        for (ui32 i = 0; i < Heap.size(); ++i) {
            Pos[Heap[i].second] = i;
        }
    }

    void Pop() {
        Y_ABORT_UNLESS(!Heap.empty());
        Pos.erase(Heap[0].second);
        if (Heap.size() > 1) {
            Heap[0] = std::move(Heap.back());
            Pos[Heap[0].second] = 0;
        }
        Heap.pop_back();
        if (!Heap.empty()) {
            SiftDown(0);
        }
    }

    // O(log N) targeted removal; no-op if shardIdx not present.
    void Erase(const TShardIdx& shardIdx) {
        auto posIt = Pos.find(shardIdx);
        if (posIt == Pos.end()) {
            return;
        }
        const ui32 i = posIt->second;
        Pos.erase(posIt);
        const ui32 last = Heap.size() - 1;
        if (i < last) {
            Heap[i] = std::move(Heap[last]);
            Pos[Heap[i].second] = i;
            Heap.pop_back();
            SiftUp(i);
            SiftDown(i);
        } else {
            Heap.pop_back();
        }
    }

    bool Empty() const {
        return Heap.empty();
    }
    bool Contains(const TShardIdx& shardIdx) const {
        return Pos.contains(shardIdx);
    }
    void Clear() {
        Heap.clear(); Pos.clear();
    }
    const TEntry& Top() const {
        Y_ABORT_UNLESS(!Heap.empty());
        return Heap[0];
    }
    const TVector<TEntry>& Container() const {
        return Heap;
    }

    bool IsValidHeap() const {
        if (Pos.size() != Heap.size()) {
            return false;
        }
        for (ui32 i = 0; i < Heap.size(); ++i) {
            auto it = Pos.find(Heap[i].second);
            if (it == Pos.end() || it->second != i) {
                return false;
            }
        }
        return std::is_heap(Heap.begin(), Heap.end(), TGreater{});
    }

private:
    struct TGreater {
        bool operator()(const TEntry& a, const TEntry& b) const {
            return a.first > b.first;
        }
    };

    void SwapAt(ui32 i, ui32 j) {
        std::swap(Heap[i], Heap[j]);
        Pos[Heap[i].second] = i;
        Pos[Heap[j].second] = j;
    }

    void SiftUp(ui32 i) {
        while (i > 0) {
            const ui32 p = (i - 1) >> 1;
            if (Heap[p].first <= Heap[i].first) {
                break;
            }
            SwapAt(i, p);
            i = p;
        }
    }

    void SiftDown(ui32 i) {
        const ui32 n = Heap.size();
        while (true) {
            ui32 best = i;
            const ui32 l = 2*i + 1, r = l + 1;
            if (l < n && Heap[l].first < Heap[best].first) {
                best = l;
            }
            if (r < n && Heap[r].first < Heap[best].first) {
                best = r;
            }
            if (best == i) {
                break;
            }
            SwapAt(i, best);
            i = best;
        }
    }

    TVector<TEntry> Heap;
    THashMap<TShardIdx, ui32> Pos;
};


} // namespace NSchemeShard
} // namespace NKikimr
