#pragma once

#include "olap/schema/schema.h"
#include "olap/schema/update.h"
#include "schemeshard_identificators.h"
#include "schemeshard_info_types_helpers.h"
#include "schemeshard_path_element.h"
#include "schemeshard_schema.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_types.h"

#include <util/generic/yexception.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/protos/yql_translation_settings.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_table_column.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/util/counted_leaky_bucket.h>
#include <ydb/core/util/pb.h>

#include <ydb/library/login/protos/login.pb.h>

#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NSchemeShard {
using namespace NTableIndex;

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


struct TBindingsRoomsChange {
    TChannelsBindings ChannelsBindings;
    NKikimrSchemeOp::TPartitionConfig PerShardConfig;
    bool ChannelsBindingsUpdated = false;
};

using TChannelsMapping = TVector<TString>; // channel idx -> storage pool name

/**
 * Maps original channels bindings to possible updates
 */
using TBindingsRoomsChanges = TMap<TChannelsMapping, TBindingsRoomsChange>;

TChannelsMapping GetPoolsMapping(const TChannelsBindings& bindings);

struct TTableShardInfo {
    TShardIdx ShardIdx = InvalidShardIdx;
    TString EndOfRange;
    TInstant LastCondErase;
    TInstant NextCondErase;
    mutable TMaybe<TDuration> LastCondEraseLag;

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
    ui32 ShardState = NKikimrTxDataShard::Unknown;

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
};

struct TAggregatedStats : public TTableAggregatedStats {
    THashMap<TPathId, TTableAggregatedStats> TableStats;

    void UpdateTableStats(TShardIdx datashardIdx, const TPathId& pathId, const TPartitionStats& newStats, TInstant now);
};

struct TSubDomainInfo;

struct TTableInfo : public TSimpleRefCount<TTableInfo> {
    using TPtr = TIntrusivePtr<TTableInfo>;
    using TCPtr = TIntrusiveConstPtr<TTableInfo>;

    struct TColumn : public NTable::TColumn {
        ui64 CreateVersion;
        ui64 DeleteVersion;
        ETableColumnDefaultKind DefaultKind = ETableColumnDefaultKind::None;
        TString DefaultValue;
        bool IsBuildInProgress = false;

        TColumn(const TString& name, ui32 id, NScheme::TTypeInfo type, const TString& typeMod, bool notNull)
            : NTable::TScheme::TColumn(name, id, type, typeMod, notNull)
            , CreateVersion(0)
            , DeleteVersion(Max<ui64>())
        {}

        TColumn()
            : NTable::TScheme::TColumn()
            , CreateVersion(0)
            , DeleteVersion(Max<ui64>())
        {}

        bool IsKey() const { return KeyOrder != Max<ui32>(); }
        bool IsDropped() const { return DeleteVersion != Max<ui64>(); }
    };

    struct TBackupRestoreResult {
        enum class EKind: ui8 {
            Backup = 0,
            Restore,
        };

        ui64 StartDateTime; // seconds
        ui64 CompletionDateTime; // seconds
        ui32 TotalShardCount;
        ui32 SuccessShardCount;
        THashMap<TShardIdx, TTxState::TShardStatus> ShardStatuses;
        ui64 DataTotalSize;
    };

    struct TAlterTableInfo : TSimpleRefCount<TAlterTableInfo> {
        using TPtr = TIntrusivePtr<TAlterTableInfo>;

        ui32 NextColumnId = 1;
        ui64 AlterVersion = 0;
        TMap<ui32, TColumn> Columns;
        TVector<ui32> KeyColumnIds;
        bool IsBackup = false;
        bool IsRestore = false;

        // Coordinated schema version for backup operations.
        // Set once by first subop that touches this AlterData via InitAlterData(opId).
        // All related operations use this pre-agreed version.
        // When all users release (CoordinatedVersionUsers becomes empty), AlterData is cleaned up.
        TMaybe<ui64> CoordinatedSchemaVersion;
        THashSet<TOperationId> CoordinatedVersionUsers;

        NKikimrSchemeOp::TTableDescription TableDescriptionDiff;
        TMaybeFail<NKikimrSchemeOp::TTableDescription> TableDescriptionFull;

        bool IsFullPartitionConfig() const {
            return TableDescriptionFull.Defined();
        }

        const NKikimrSchemeOp::TTableDescription& TableDescription() const {
            if (IsFullPartitionConfig()) {
                return *TableDescriptionFull;
            }
            return TableDescriptionDiff;
        }
        NKikimrSchemeOp::TTableDescription& TableDescription() {
            if (IsFullPartitionConfig()) {
                return *TableDescriptionFull;
            }
            return TableDescriptionDiff;
        }

        const NKikimrSchemeOp::TPartitionConfig& PartitionConfigDiff() const { return TableDescriptionDiff.GetPartitionConfig(); }
        NKikimrSchemeOp::TPartitionConfig& PartitionConfigDiff() { return *TableDescriptionDiff.MutablePartitionConfig(); }

        const NKikimrSchemeOp::TPartitionConfig& PartitionConfigFull() const { return TableDescriptionFull->GetPartitionConfig(); }
        NKikimrSchemeOp::TPartitionConfig& PartitionConfigFull() { return *TableDescriptionFull->MutablePartitionConfig(); }

        const NKikimrSchemeOp::TPartitionConfig& PartitionConfigCompatible() const {
            return TableDescription().GetPartitionConfig();
        }
        NKikimrSchemeOp::TPartitionConfig& PartitionConfigCompatible() {
            return *TableDescription().MutablePartitionConfig();
        }
    };

    using TAlterDataPtr = TAlterTableInfo::TPtr;

    ui32 NextColumnId = 1;          // Next unallocated column id
    ui64 AlterVersion = 0;
    ui64 PartitioningVersion = 0;
    TMap<ui32, TColumn> Columns;
    TVector<ui32> KeyColumnIds;
    bool IsBackup = false;
    bool IsRestore = false;
    bool IsTemporary = false;
    TActorId OwnerActorId;

    TAlterTableInfo::TPtr AlterData;

    NKikimrSchemeOp::TTableDescription TableDescription;

    NKikimrSchemeOp::TBackupTask BackupSettings;
    NKikimrSchemeOp::TRestoreTask RestoreSettings;
    TMap<TTxId, TBackupRestoreResult> BackupHistory;
    TMap<TTxId, TBackupRestoreResult> RestoreHistory;

    // Preserialized TDescribeSchemeResult with PathDescription.TablePartitions field filled
    TString PreserializedTablePartitions;
    TString PreserializedTablePartitionsNoKeys;
    // Preserialized TDescribeSchemeResult with PathDescription.Table.SplitBoundary field filled
    TString PreserializedTableSplitBoundaries;

    THashMap<TShardIdx, NKikimrSchemeOp::TPartitionConfig> PerShardPartitionConfig;

    bool IsExternalBlobsEnabled = false;

    const NKikimrSchemeOp::TPartitionConfig& PartitionConfig() const { return TableDescription.GetPartitionConfig(); }
    NKikimrSchemeOp::TPartitionConfig& MutablePartitionConfig() { return *TableDescription.MutablePartitionConfig(); }

    bool HasReplicationConfig() const { return TableDescription.HasReplicationConfig(); }
    const NKikimrSchemeOp::TTableReplicationConfig& ReplicationConfig() const { return TableDescription.GetReplicationConfig(); }
    NKikimrSchemeOp::TTableReplicationConfig& MutableReplicationConfig() { return *TableDescription.MutableReplicationConfig(); }

    bool IsAsyncReplica() const {
        switch (TableDescription.GetReplicationConfig().GetMode()) {
            case NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE:
                return false;
            default:
                return true;
        }
    }

    bool HasIncrementalBackupConfig() const { return TableDescription.HasIncrementalBackupConfig(); }
    const NKikimrSchemeOp::TTableIncrementalBackupConfig& IncrementalBackupConfig() const { return TableDescription.GetIncrementalBackupConfig(); }
    NKikimrSchemeOp::TTableIncrementalBackupConfig& MutableIncrementalBackupConfig() { return *TableDescription.MutableIncrementalBackupConfig(); }

    bool IsIncrementalRestoreTable() const {
        switch (TableDescription.GetIncrementalBackupConfig().GetMode()) {
            case NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_NONE:
                return false;
            default:
                return true;
        }
    }

    bool HasTTLSettings() const { return TableDescription.HasTTLSettings(); }
    const NKikimrSchemeOp::TTTLSettings& TTLSettings() const { return TableDescription.GetTTLSettings(); }
    bool IsTTLEnabled() const { return HasTTLSettings() && TTLSettings().HasEnabled(); }

    NKikimrSchemeOp::TTTLSettings& MutableTTLSettings() {
        TTLColumnId.Clear();
        return *TableDescription.MutableTTLSettings();
    }

    ui32 GetTTLColumnId() const {
        if (!IsTTLEnabled()) {
            return Max<ui32>();
        }

        if (!TTLColumnId) {
            for (const auto& [id, col] : Columns) {
                if (!col.IsDropped() && col.Name == TTLSettings().GetEnabled().GetColumnName()) {
                    TTLColumnId = id;
                    break;
                }
            }
        }

        if (!TTLColumnId) {
            TTLColumnId = Max<ui32>();
        }

        return *TTLColumnId;
    }

private:
    using TPartitionsVec = TVector<TTableShardInfo>;

    struct TSortByNextCondErase {
        using TIterator = TPartitionsVec::iterator;

        bool operator()(TIterator left, TIterator right) const {
            return left->NextCondErase > right->NextCondErase;
        }
    };

    TPartitionsVec Partitions;
    THashMap<TShardIdx, ui64> Shard2PartitionIdx; // shardIdx -> index in Partitions
    TPriorityQueue<TPartitionsVec::iterator, TVector<TPartitionsVec::iterator>, TSortByNextCondErase> CondEraseSchedule;
    THashMap<TShardIdx, TActorId> InFlightCondErase; // shard to pipe client
    mutable TMaybe<ui32> TTLColumnId;
    THashSet<TOperationId> SplitOpsInFlight;
    THashMap<TOperationId, TVector<TShardIdx>> ShardsInSplitMergeByOpId;
    THashMap<TShardIdx, TOperationId> ShardsInSplitMergeByShards;
    ui64 ExpectedPartitionCount = 0; // number of partitions after all in-flight splits/merges are finished
    TAggregatedStats Stats;
    bool ShardsStatsDetached = false;

    TPartitionsVec::iterator FindPartition(const TShardIdx& shardIdx) {
        auto it = Shard2PartitionIdx.find(shardIdx);
        if (it == Shard2PartitionIdx.end()) {
            return Partitions.end();
        }

        const auto partitionIdx = it->second;
        if (partitionIdx >= Partitions.size()) {
            return Partitions.end();
        }

        return Partitions.begin() + partitionIdx;
    }

public:
    TTableInfo() = default;

    explicit TTableInfo(TAlterTableInfo&& alterData)
        : NextColumnId(alterData.NextColumnId)
        , AlterVersion(alterData.AlterVersion)
        , Columns(std::move(alterData.Columns))
        , KeyColumnIds(std::move(alterData.KeyColumnIds))
        , IsBackup(alterData.IsBackup)
        , IsRestore(alterData.IsRestore)
    {
        TableDescription.Swap(alterData.TableDescriptionFull.Get());
        IsExternalBlobsEnabled = PartitionConfigHasExternalBlobsEnabled(TableDescription.GetPartitionConfig());
    }

    static TTableInfo::TPtr DeepCopy(const TTableInfo& other) {
        TTableInfo::TPtr copy(new TTableInfo(other));
        // rebuild conditional erase schedule since it uses iterators
        copy->CondEraseSchedule.clear();
        for (ui32 i = 0; i < copy->Partitions.size(); ++i) {
            copy->CondEraseSchedule.push(copy->Partitions.begin() + i);
        }

        return copy;
    }

    struct TCreateAlterDataFeatureFlags {
        bool EnableTablePgTypes;
        bool EnableTableDatetime64;
        bool EnableParameterizedDecimal;
        bool EnableSetColumnConstraint = false; // This flag is used in alter table operation only
    };

    static TAlterDataPtr CreateAlterData(
        TPtr source,
        NKikimrSchemeOp::TTableDescription& descr,
        const NScheme::TTypeRegistry& typeRegistry,
        const TSchemeLimits& limits, const TSubDomainInfo& subDomain,
        const TCreateAlterDataFeatureFlags& featureFlags,
        TString& errStr, const THashSet<TString>& localSequences = {});

    static ui32 ShardsToCreate(const NKikimrSchemeOp::TTableDescription& descr) {
        if (descr.HasUniformPartitionsCount()) {
            return descr.GetUniformPartitionsCount();
        } else {
            return descr.SplitBoundarySize() + 1;
        }
    }

    void ResetDescriptionCache();
    TVector<ui32> FillDescriptionCache(TPathElement::TPtr pathInfo);

    void SetRoom(const TStorageRoom& room) {
        // WARNING: this is legacy support code
        // StorageRooms from per-table partition config are only used for
        // tablets that don't have per-shard patches. During migration we
        // expect to only ever create single-room shards, which cannot have
        // their storage config altered, so per-table and per-shard rooms
        // cannot diverge. These settings will eventually become dead weight,
        // only useful for ancient shards, after which may remove this code.
        Y_ENSURE(room.GetId() == 0);
        auto rooms = MutablePartitionConfig().MutableStorageRooms();
        rooms->Clear();
        rooms->Add()->CopyFrom(room);
    }


    // InitAlterData without tracking - for loading persisted state (init.cpp)
    void InitAlterData() {
        if (!AlterData) {
            AlterData = new TTableInfo::TAlterTableInfo;
            AlterData->AlterVersion = AlterVersion + 1;
            AlterData->NextColumnId = NextColumnId;
        }
    }

    // InitAlterData with tracking - for coordinated versioning operations.
    // Tracks which operations are using this AlterData. When all release, it's cleaned up.
    // Also ensures CoordinatedSchemaVersion is set in TableDescriptionFull for persistence.
    void InitAlterData(const TOperationId& opId) {
        // If AlterData exists but has no users, it's stale from restart - reset it
        if (AlterData && AlterData->CoordinatedVersionUsers.empty()) {
            AlterData.Reset();
        }
        if (!AlterData) {
            AlterData = new TTableInfo::TAlterTableInfo;
            AlterData->AlterVersion = AlterVersion + 1;
            AlterData->CoordinatedSchemaVersion = AlterVersion + 1;
            AlterData->NextColumnId = NextColumnId;
        }
        // Ensure TableDescriptionFull exists and has CoordinatedSchemaVersion set for persistence
        if (!AlterData->TableDescriptionFull) {
            AlterData->TableDescriptionFull = NKikimrSchemeOp::TTableDescription();
        }
        AlterData->TableDescriptionFull->SetCoordinatedSchemaVersion(*AlterData->CoordinatedSchemaVersion);
        AlterData->CoordinatedVersionUsers.insert(opId);
    }

    // Release AlterData after coordinated versioning operation completes.
    // When all users release, AlterData is cleaned up.
    // Returns true if AlterData was fully released (all users done).
    bool ReleaseAlterData(const TOperationId& opId) {
        if (!AlterData) {
            return false;
        }
        AlterData->CoordinatedVersionUsers.erase(opId);
        if (AlterData->CoordinatedVersionUsers.empty()) {
            AlterData.Reset();
            return true;  // Caller should clear AlterTableFull from DB
        }
        return false;
    }

    void PrepareAlter(TAlterDataPtr alterData) {
        Y_ENSURE(alterData, "No alter data at Alter prepare");
        Y_ENSURE(alterData->AlterVersion == AlterVersion + 1);
        AlterData = alterData;
    }

    void FinishAlter();

#if 1 // legacy
    TString SerializeAlterExtraData() const;

    void DeserializeAlterExtraData(const TString& str);
#endif

    void SetPartitioning(TVector<TTableShardInfo>&& newPartitioning);

    const TVector<TTableShardInfo>& GetPartitions() const {
        return Partitions;
    }

    const TAggregatedStats& GetStats() const {
        return Stats;
    }

    bool IsShardsStatsDetached() const {
        return ShardsStatsDetached;
    }
    void DetachShardsStats() {
        ShardsStatsDetached = true;
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

    void RegisterSplitMergeOp(TOperationId txId, const TTxState& txState);

    bool IsShardInSplitMergeOp(TShardIdx idx) const;
    void FinishSplitMergeOp(TOperationId txId);
    void AbortSplitMergeOp(TOperationId txId);

    const THashSet<TOperationId>& GetSplitOpsInFlight() const {
        return SplitOpsInFlight;
    }

    const THashMap<TShardIdx, ui64>& GetShard2PartitionIdx() const {
        return Shard2PartitionIdx;
    }

    ui64 GetExpectedPartitionCount() const {
        return ExpectedPartitionCount;
    }

    bool TryAddShardToMerge(const TSplitSettings& splitSettings,
                            const TForceShardSplitSettings& forceShardSplitSettings,
                            TShardIdx shardIdx, TVector<TShardIdx>& shardsToMerge,
                            THashSet<TTabletId>& partOwners, ui64& totalSize, float& totalLoad,
                            float cpuUsageThreshold, const TTableInfo* mainTableForIndex, TInstant now, TString& reason) const;

    bool CheckCanMergePartitions(const TSplitSettings& splitSettings,
                                 const TForceShardSplitSettings& forceShardSplitSettings,
                                 TShardIdx shardIdx, const TTabletId& tabletId, TVector<TShardIdx>& shardsToMerge,
                                 const TTableInfo* mainTableForIndex, TInstant now, TString& reason) const;

    /**
     * Check if the given partition should be split by load.
     *
     * @param[in] splitSettings The current split settings
     * @param[in] shardIdx The shard index
     * @param[in] currentCpuUsage The current CPU usage to use for checking the split conditions
     * @param[in] mainTableForIndex The parent table (set only for index tables)
     * @param[out] reason Receives the human readable explanation for the decision
     *
     * @return True if the given partition should be split by load
     */
    bool CheckSplitByLoad(
        const TSplitSettings& splitSettings,
        const TShardIdx& shardIdx,
        ui64 currentCpuUsage,
        const TTableInfo* mainTableForIndex,
        TString& reason
    ) const;

    bool IsSplitBySizeEnabled(const TForceShardSplitSettings& params) const {
        // Respect unspecified SizeToSplit when force shard splits are disabled
        if (params.DisableForceShardSplit && PartitionConfig().GetPartitioningPolicy().GetSizeToSplit() == 0) {
            return false;
        }
        // Auto split is always enabled, unless table is using external blobs
        return (IsExternalBlobsEnabled == false);
    }

    bool IsMergeBySizeEnabled(const TForceShardSplitSettings& params) const {
        // Auto merge is only enabled when auto split is also enabled
        if (!IsSplitBySizeEnabled(params)) {
            return false;
        }
        // We want auto merge enabled when user has explicitly specified the
        // size to split and the minimum partitions count.
        if (PartitionConfig().GetPartitioningPolicy().GetSizeToSplit() > 0 &&
            PartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount() != 0)
        {
            return true;
        }
        // We also want auto merge enabled when table has more shards than the
        // specified maximum number of partitions. This way when something
        // splits by size over the limit we merge some smaller partitions.
        return Partitions.size() > GetMaxPartitionsCount() && !params.DisableForceShardSplit;
    }

    NKikimrSchemeOp::TSplitByLoadSettings GetEffectiveSplitByLoadSettings(
            const TTableInfo* mainTableForIndex) const
    {
        NKikimrSchemeOp::TSplitByLoadSettings settings;

        if (mainTableForIndex) {
            // Merge main table settings first
            // Index settings will override these
            settings.MergeFrom(
                mainTableForIndex->PartitionConfig()
                .GetPartitioningPolicy()
                .GetSplitByLoadSettings());
        }

        // Merge local table settings last, they take precedence
        settings.MergeFrom(
            PartitionConfig()
            .GetPartitioningPolicy()
            .GetSplitByLoadSettings());

        return settings;
    }

    bool IsSplitByLoadEnabled(const TTableInfo* mainTableForIndex) const {
        // We cannot split when external blobs are enabled
        if (IsExternalBlobsEnabled) {
            return false;
        }

        const auto& policy = PartitionConfig().GetPartitioningPolicy();
        if (policy.HasSplitByLoadSettings() && policy.GetSplitByLoadSettings().HasEnabled()) {
            // Always prefer any explicit setting
            return policy.GetSplitByLoadSettings().GetEnabled();
        }

        if (mainTableForIndex) {
            // Enable by default for indexes, when enabled for the main table
            // TODO: consider always enabling by default
            const auto& mainPolicy = mainTableForIndex->PartitionConfig().GetPartitioningPolicy();
            return mainPolicy.GetSplitByLoadSettings().GetEnabled();
        }

        // Disable by default for normal tables
        return false;
    }

    bool IsMergeByLoadEnabled(const TTableInfo* mainTableForIndex) const {
        return IsSplitByLoadEnabled(mainTableForIndex);
    }

    ui64 GetShardSizeToSplit(const TForceShardSplitSettings& params) const {
        if (!IsSplitBySizeEnabled(params)) {
            return Max<ui64>();
        }
        ui64 threshold = PartitionConfig().GetPartitioningPolicy().GetSizeToSplit();
        if (params.DisableForceShardSplit) {
            if (threshold == 0) {
                return Max<ui64>();
            }
        } else {
            if (threshold == 0 || threshold >= params.ForceShardSplitDataSize) {
                return params.ForceShardSplitDataSize;
            }
        }
        return threshold;
    }

    ui64 GetSizeToMerge(const TForceShardSplitSettings& params) const {
        if (!IsMergeBySizeEnabled(params)) {
            // Disable auto-merge by default
            return 0;
        } else {
            return GetShardSizeToSplit(params) / 2;
        }
    }

    ui64 GetMinPartitionsCount() const {
        ui64 val = PartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount();
        return val == 0 ? 1 : val;
    }

    ui64 GetMaxPartitionsCount() const {
        ui64 val = PartitionConfig().GetPartitioningPolicy().GetMaxPartitionsCount();
        return val == 0 ? 32*1024 : val;
    }

    bool IsForceSplitBySizeShardIdx(TShardIdx shardIdx, const TForceShardSplitSettings& params) const {
        if (!Stats.PartitionStats.contains(shardIdx) || params.DisableForceShardSplit) {
            return false;
        }
        const auto& stats = Stats.PartitionStats.at(shardIdx);
        return stats.DataSize >= params.ForceShardSplitDataSize;
    }

    bool ShouldSplitBySize(ui64 dataSize, const TForceShardSplitSettings& params, TString& reason) const {
        // Don't split/merge backup tables
        if (IsBackup) {
            return false;
        }

        if (!IsSplitBySizeEnabled(params)) {
            return false;
        }
        // When shard is over the maximum size we split even when over max partitions
        if (dataSize >= params.ForceShardSplitDataSize && !params.DisableForceShardSplit) {
            reason = TStringBuilder() << "force split by size ("
                << "shardSize: " << dataSize << ", "
                << "maxShardSize: " << params.ForceShardSplitDataSize << ")";

            return true;
        }
        // Otherwise we split when we may add one more partition
        if (Partitions.size() < GetMaxPartitionsCount() && dataSize >= GetShardSizeToSplit(params)) {
            reason = TStringBuilder() << "split by size ("
                << "shardCount: " << Partitions.size() << ", "
                << "maxShardCount: " << GetMaxPartitionsCount() << ", "
                << "shardSize: " << dataSize << ", "
                << "maxShardSize: " << GetShardSizeToSplit(params) << ")";

            return true;
        }

        return false;
    }

    bool NeedRecreateParts() const {
        if (!AlterData) {
            return false;
        }

        auto srcFollowerParams = std::tuple<ui64, bool, ui32>(
                                         PartitionConfig().GetFollowerCount(),
                                         PartitionConfig().GetAllowFollowerPromotion(),
                                         PartitionConfig().GetCrossDataCenterFollowerCount()
            );

        auto alterFollowerParams = std::tuple<ui64, bool, ui32>(
                                         AlterData->PartitionConfigCompatible().GetFollowerCount(),
                                         AlterData->PartitionConfigCompatible().GetAllowFollowerPromotion(),
                                         AlterData->PartitionConfigCompatible().GetCrossDataCenterFollowerCount()

            );

        auto equals_proto_array = [] (const auto& left, const auto& right) {
            if (left.size() != right.size()) {
                return false;
            }

            for (decltype(right.size()) i = 0; i < right.size(); ++i) {
                if (!google::protobuf::util::MessageDifferencer::Equals(left[i], right[i])) {
                    return false;
                }
            }

            return true;
        };



        return srcFollowerParams != alterFollowerParams
            || !equals_proto_array(
                   PartitionConfig().GetFollowerGroups(),
                   AlterData->PartitionConfigCompatible().GetFollowerGroups());
    }

    const TTableShardInfo* GetScheduledCondEraseShard() const {
        if (CondEraseSchedule.empty()) {
            return nullptr;
        }

        return CondEraseSchedule.top();
    }

    const auto& GetInFlightCondErase() const {
        return InFlightCondErase;
    }

    auto& GetInFlightCondErase() {
        return InFlightCondErase;
    }

    void AddInFlightCondErase(const TShardIdx& shardIdx) {
        const auto* shardInfo = GetScheduledCondEraseShard();
        Y_ENSURE(shardInfo && shardIdx == shardInfo->ShardIdx);

        InFlightCondErase[shardIdx] = TActorId();
        CondEraseSchedule.pop();
    }

    void RescheduleCondErase(const TShardIdx& shardIdx) {
        Y_ENSURE(InFlightCondErase.contains(shardIdx));

        auto it = FindPartition(shardIdx);
        Y_ENSURE(it != Partitions.end());

        CondEraseSchedule.push(it);
        InFlightCondErase.erase(shardIdx);
    }

    void UpdateNextCondErase(const TShardIdx& shardIdx, const TInstant& now, const TDuration& next) {
        auto it = FindPartition(shardIdx);
        Y_ENSURE(it != Partitions.end());

        it->LastCondErase = now;
        it->NextCondErase = now + next;
        it->LastCondEraseLag = TDuration::Zero();
    }

    bool IsUsingSequence(const TString& name) {
        for (const auto& pr : Columns) {
            if (pr.second.DefaultKind == ETableColumnDefaultKind::FromSequence &&
                pr.second.DefaultValue == name)
            {
                return true;
            }
        }
        return false;
    }
};

struct TTopicStats {
    TMessageSeqNo SeqNo;

    ui64 DataSize = 0;
    ui64 UsedReserveSize = 0;

    TString ToString() const {
        return TStringBuilder() << "TTopicStats {"
                                << " DataSize: " << DataSize
                                << " UsedReserveSize: " << UsedReserveSize
                                << " }";
    }
};

struct TTopicTabletInfo : TSimpleRefCount<TTopicTabletInfo> {
    using TPtr = TIntrusivePtr<TTopicTabletInfo>;
    using TKeySchema = TVector<NScheme::TTypeInfo>;

    struct TKeyRange {
        TMaybe<TString> FromBound;
        TMaybe<TString> ToBound;

        void SerializeToProto(NKikimrPQ::TPartitionKeyRange& proto) const;
        void DeserializeFromProto(const NKikimrPQ::TPartitionKeyRange& proto);
    };

    struct TTopicPartitionInfo {
        // Partition id
        ui32 PqId = 0;
        ui32 GroupId = 0;
        // AlterVersion of topic which partition was updated last.
        ui64 AlterVersion = 0;
        // AlterVersion of topic which partition was created.
        // For example, it required for generate "timebased" offsets for kinesis protocol.
        ui64 CreateVersion;

        NKikimrPQ::ETopicPartitionStatus Status = NKikimrPQ::ETopicPartitionStatus::Active;

        TMaybe<TKeyRange> KeyRange;

        // Split and merge operations form the partitions graph. Each partition created in this way has a parent
        // partition. In turn, the parent partition knows about the partitions formed after the split and merge
        // operations.
        TSet<ui32> ParentPartitionIds;
        TSet<ui32> ChildPartitionIds;

        TShardIdx ShardIdx;

        void SetStatus(const TActorContext& ctx, ui32 value) {
            if (value >= NKikimrPQ::ETopicPartitionStatus::Active &&
                value <= NKikimrPQ::ETopicPartitionStatus::Deleted) {
                Status = static_cast<NKikimrPQ::ETopicPartitionStatus>(value);
            } else {
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Read unknown topic partition status value " << value);
                Status = NKikimrPQ::ETopicPartitionStatus::Active;
            }
        }
    };

    TVector<TAutoPtr<TTopicPartitionInfo>> Partitions;

    size_t PartsCount() const {
        return Partitions.size();
    }
};

struct TAdoptedShard {
    ui64 PrevOwner;
    TLocalShardIdx PrevShardIdx;
};

struct TShardInfo {
    TTabletId TabletID = InvalidTabletId;
    TTxId CurrentTxId = InvalidTxId; ///< @note we support only one modifying transaction on shard at time
    TPathId PathId = InvalidPathId;
    TTabletTypes::EType TabletType = ETabletType::TypeInvalid;
    TChannelsBindings BindedChannels;

    TShardInfo(TTxId txId, TPathId pathId, TTabletTypes::EType type)
       : CurrentTxId(txId)
       , PathId(pathId)
       , TabletType(type)
    {}

    TShardInfo() = default;
    TShardInfo(const TShardInfo& other) = default;
    TShardInfo &operator=(const TShardInfo& other) = default;

    TShardInfo&& WithTabletID(TTabletId tabletId) && {
        TabletID = tabletId;
        return std::move(*this);
    }

    TShardInfo WithTabletID(TTabletId tabletId) const & {
        TShardInfo copy = *this;
        copy.TabletID = tabletId;
        return copy;
    }

    TShardInfo&& WithTabletType(TTabletTypes::EType tabletType) && {
        TabletType = tabletType;
        return std::move(*this);
    }

    TShardInfo WithTabletType(TTabletTypes::EType tabletType) const & {
        TShardInfo copy = *this;
        copy.TabletType = tabletType;
        return copy;
    }

    TShardInfo&& WithBindedChannels(TChannelsBindings bindedChannels) && {
        BindedChannels = std::move(bindedChannels);
        return std::move(*this);
    }

    TShardInfo WithBindedChannels(TChannelsBindings bindedChannels) const & {
        TShardInfo copy = *this;
        copy.BindedChannels = std::move(bindedChannels);
        return copy;
    }

    static TShardInfo RtmrPartitionInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::RTMRPartition);
    }

    static TShardInfo SolomonPartitionInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::KeyValue);
    }

    static TShardInfo DataShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::DataShard);
    }

    static TShardInfo PersQShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::PersQueue);
    }

    static TShardInfo PQBalancerShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::PersQueueReadBalancer);
    }

    static TShardInfo BlockStoreVolumeInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStoreVolume);
    }

    static TShardInfo BlockStorePartitionInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartition);
    }

    static TShardInfo BlockStorePartition2Info(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartition2);
    }

    static TShardInfo BlockStoreVolumeDirectInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStoreVolumeDirect);
    }

    static TShardInfo BlockStorePartitionDirectInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartitionDirect);
    }

    static TShardInfo FileStoreInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::FileStore);
    }

    static TShardInfo KesusInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::Kesus);
    }

    static TShardInfo ColumnShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::ColumnShard);
    }

    static TShardInfo SequenceShardInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::SequenceShard);
    }

    static TShardInfo ReplicationControllerInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::ReplicationController);
    }

    static TShardInfo BlobDepotInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlobDepot);
    }
};

/**
 * TTopicInfo -> TTopicTabletInfo -> TTopicPartitionInfo
 *
 * Each topic may contains many tablets.
 * Each tablet may serve many partitions.
 */
struct TTopicInfo : TSimpleRefCount<TTopicInfo> {
    using TPtr = TIntrusivePtr<TTopicInfo>;
    using TKeySchema = TTopicTabletInfo::TKeySchema;

    struct TPartitionToAdd {
        using TKeyRange = TTopicTabletInfo::TKeyRange;

        ui32 PartitionId;
        ui32 GroupId;
        TMaybe<TKeyRange> KeyRange;
        THashSet<ui32> ParentPartitionIds;

        explicit TPartitionToAdd(ui32 partitionId, ui32 groupId, const TMaybe<TKeyRange>& keyRange = Nothing(),
                                 const THashSet<ui32>& parentPartitionIds = {})
            : PartitionId(partitionId)
            , GroupId(groupId)
            , KeyRange(keyRange)
            , ParentPartitionIds(parentPartitionIds) {
        }

        bool operator==(const TPartitionToAdd& rhs) const {
            return PartitionId == rhs.PartitionId
                && GroupId == rhs.GroupId;
        }

        struct THash {
            inline size_t operator()(const TPartitionToAdd& obj) const {
                const ::THash<ui32> hashFn;
                return CombineHashes(hashFn(obj.PartitionId), hashFn(obj.GroupId));
            }
        };
    };

    ui64 TotalGroupCount = 0;
    ui64 TotalPartitionCount = 0;
    ui32 NextPartitionId = 0;
    THashSet<TPartitionToAdd, TPartitionToAdd::THash> PartitionsToAdd;
    THashSet<ui32> PartitionsToDelete;
    THashMap<ui32, TMaybe<TTopicTabletInfo::TKeyRange>> KeyRangesToChange;
    ui32 MaxPartsPerTablet = 0;
    ui64 AlterVersion = 0;
    TString TabletConfig;
    TString BootstrapConfig;
    THashMap<TShardIdx, TTopicTabletInfo::TPtr> Shards; // key - shardIdx
    TKeySchema KeySchema;
    TTopicInfo::TPtr AlterData; // changes to be applied
    TTabletId BalancerTabletID = InvalidTabletId;
    TShardIdx BalancerShardIdx = InvalidShardIdx;
    THashMap<ui32, TTopicTabletInfo::TTopicPartitionInfo*> Partitions;
    size_t ActivePartitionCount = 0;
    THashSet<ui32> OffloadDonePartitions;

    TString PreSerializedPathDescription; // Cached path description
    TString PreSerializedPartitionsDescription; // Cached partition description

    TTopicStats Stats;

    void AddPartition(TShardIdx shardIdx, TTopicTabletInfo::TTopicPartitionInfo* partition) {
        partition->ShardIdx = shardIdx;

        TTopicTabletInfo::TPtr& pqShard = Shards[shardIdx];
        if (!pqShard) {
            pqShard.Reset(new TTopicTabletInfo());
        }
        pqShard->Partitions.push_back(partition);
        Partitions[partition->PqId] = pqShard->Partitions.back().Get();
    }

    void UpdateSplitMergeGraph(const TTopicTabletInfo::TTopicPartitionInfo& partition) {
        for (const auto parent : partition.ParentPartitionIds) {
            auto it = Partitions.find(parent);
            Y_ENSURE(it != Partitions.end(),
                     "Partition " << partition.GroupId << " has parent partition " << parent << " which doesn't exists");
            it->second->ChildPartitionIds.emplace(partition.PqId);
        }
    }

    void InitSplitMergeGraph() {
        for (const auto& [_, partition] : Partitions) {
            UpdateSplitMergeGraph(*partition);
        }
    }

    bool SupportSplitMerge() {
        return KeySchema.empty();
    }

    bool FillKeySchema(const NKikimrPQ::TPQTabletConfig& tabletConfig, TString& error);
    bool FillKeySchema(const TString& tabletConfig);

    bool HasBalancer() const { return bool(BalancerTabletID); }

    ui32 GetTotalPartitionCountWithAlter() const {
        ui32 res = 0;
        for (const auto& shard : Shards) {
            res += shard.second->PartsCount();
        }
        return res;
    }

    ui32 ExpectedShardCount() const {

        Y_ENSURE(TotalPartitionCount);
        Y_ENSURE(MaxPartsPerTablet);

        ui32 partsPerTablet = MaxPartsPerTablet;
        ui32 pqTabletCount = TotalPartitionCount / partsPerTablet;
        if (TotalPartitionCount % partsPerTablet) {
            ++pqTabletCount;
        }
        return pqTabletCount;
    }

    ui32 ShardCount() const {
        return Shards.size();
    }

    TVector<std::pair<TShardIdx, TTopicTabletInfo::TTopicPartitionInfo*>> GetPartitions() {
        TVector<std::pair<TShardIdx, TTopicTabletInfo::TTopicPartitionInfo*>> partitions;
        partitions.reserve(TotalPartitionCount);

        for (auto& [shardIdx, tabletInfo] : Shards) {
            for (const auto& partitionInfo : tabletInfo->Partitions) {
                partitions.push_back({shardIdx, partitionInfo.Get()});
            }
        }

        std::sort(partitions.begin(), partitions.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.second->PqId < rhs.second->PqId;
        });

        return partitions;
    }

    NKikimrPQ::TPQTabletConfig GetTabletConfig() const {
        NKikimrPQ::TPQTabletConfig tabletConfig;
        if (!TabletConfig.empty()) {
            bool parseOk = ParseFromStringNoSizeLimit(tabletConfig, TabletConfig);
            Y_ENSURE(parseOk, "Previously serialized pq tablet config cannot be parsed");
        }
        return tabletConfig;
    }

    void PrepareAlter(TTopicInfo::TPtr alterData) {
        Y_ENSURE(alterData, "No alter data at Alter prepare");
        alterData->AlterVersion = AlterVersion + 1;
        Y_ENSURE(alterData->TotalGroupCount);
        Y_ENSURE(alterData->TotalPartitionCount);
        Y_ENSURE(0 < alterData->ActivePartitionCount && alterData->ActivePartitionCount <= alterData->TotalPartitionCount);
        Y_ENSURE(alterData->NextPartitionId);
        Y_ENSURE(alterData->MaxPartsPerTablet);
        alterData->KeySchema = KeySchema;
        alterData->BalancerTabletID = BalancerTabletID;
        alterData->BalancerShardIdx = BalancerShardIdx;
        AlterData = alterData;
    }

    void FinishAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter complete");
        TotalGroupCount = AlterData->TotalGroupCount;
        NextPartitionId = AlterData->NextPartitionId;
        TotalPartitionCount = AlterData->TotalPartitionCount;
        ActivePartitionCount = AlterData->ActivePartitionCount;
        MaxPartsPerTablet = AlterData->MaxPartsPerTablet;
        if (!AlterData->TabletConfig.empty())
            TabletConfig = std::move(AlterData->TabletConfig);
        ++AlterVersion;
        Y_ENSURE(BalancerTabletID == AlterData->BalancerTabletID || !HasBalancer());
        Y_ENSURE(AlterData->HasBalancer());
        Y_ENSURE(AlterData->BalancerShardIdx);
        KeySchema = AlterData->KeySchema;
        BalancerTabletID = AlterData->BalancerTabletID;
        BalancerShardIdx = AlterData->BalancerShardIdx;
        AlterData.Reset();

        Partitions.clear();
        for (const auto& [_, shard] : Shards) {
            for (auto& partition : shard->Partitions) {
                Partitions[partition->PqId] = partition.Get();
            }
        }

        InitSplitMergeGraph();
    }
};

struct TRtmrPartitionInfo: TSimpleRefCount<TRtmrPartitionInfo> {
    using TPtr = TIntrusivePtr<TRtmrPartitionInfo>;
    TGUID Id;
    ui64 BusKey;
    TShardIdx ShardIdx;
    TTabletId TabletId;

    TRtmrPartitionInfo(TGUID id, ui64 busKey, TShardIdx shardIdx, TTabletId tabletId = InvalidTabletId):
        Id(id), BusKey(busKey), ShardIdx(shardIdx), TabletId(tabletId)
    {}
};

struct TRtmrVolumeInfo: TSimpleRefCount<TRtmrVolumeInfo> {
    using TPtr = TIntrusivePtr<TRtmrVolumeInfo>;

    THashMap<TShardIdx, TRtmrPartitionInfo::TPtr> Partitions;
};

struct TSolomonPartitionInfo: TSimpleRefCount<TSolomonPartitionInfo> {
    using TPtr = TIntrusivePtr<TSolomonPartitionInfo>;
    ui64 PartitionId;
    TTabletId TabletId;

    TSolomonPartitionInfo(ui64 partId, TTabletId tabletId = InvalidTabletId)
        : PartitionId(partId)
        , TabletId(tabletId)
    {}
};

struct TSolomonVolumeInfo: TSimpleRefCount<TSolomonVolumeInfo> {
    using TPtr = TIntrusivePtr<TSolomonVolumeInfo>;

    THashMap<TShardIdx, TSolomonPartitionInfo::TPtr> Partitions;
    ui64 Version;
    TSolomonVolumeInfo::TPtr AlterData;

    TSolomonVolumeInfo(ui64 version)
        : Version(version)
    {
    }

    TSolomonVolumeInfo::TPtr CreateAlter() const {
        return CreateAlter(Version + 1);
    }

    TSolomonVolumeInfo::TPtr CreateAlter(ui64 version) const {
        Y_ENSURE(Version < version);
        TSolomonVolumeInfo::TPtr alter = new TSolomonVolumeInfo(*this);
        alter->Version = version;
        return alter;
    }
};


using TSchemeQuota = TCountedLeakyBucket;

struct TSchemeQuotas : public TVector<TSchemeQuota> {
    mutable size_t LastKnownSize = 0;
};

enum class EUserFacingStorageType {
    Ssd,
    Hdd,
    Ignored
};

struct IQuotaCounters {
    virtual void ChangeStreamShardsCount(i64 delta) = 0;
    virtual void ChangeStreamShardsQuota(i64 delta) = 0;
    virtual void ChangeStreamReservedStorageQuota(i64 delta) = 0;
    virtual void ChangeStreamReservedStorageCount(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesDataBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesIndexBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesTotalBytes(i64 delta) = 0;
    virtual void AddDiskSpaceTables(EUserFacingStorageType storageType, ui64 data, ui64 index) = 0;
    virtual void ChangeDiskSpaceTopicsTotalBytes(ui64 value) = 0;
    virtual void ChangeDiskSpaceQuotaExceeded(i64 delta) = 0;
    virtual void ChangeDiskSpaceHardQuotaBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceSoftQuotaBytes(i64 delta) = 0;
    virtual void AddDiskSpaceSoftQuotaBytes(EUserFacingStorageType storageType, ui64 addend) = 0;
    virtual void ChangePathCount(i64 delta) = 0;
    virtual void SetPathCount(ui64 value) = 0;
    virtual void SetPathsQuota(ui64 value) = 0;
    virtual void ChangeShardCount(i64 delta) = 0;
    virtual void SetShardCount(ui64 value) = 0;
    virtual void SetShardsQuota(ui64 value) = 0;
};

enum class EPathCategory : ui8 {
    Regular = 0,
    Backup,
    System,
};

struct TSubDomainInfo: TSimpleRefCount<TSubDomainInfo> {
    using TPtr = TIntrusivePtr<TSubDomainInfo>;
    using TConstPtr = TIntrusiveConstPtr<TSubDomainInfo>;

    struct TDiskSpaceUsage {
        struct TTables {
            ui64 TotalSize = 0;
            ui64 DataSize = 0;
            ui64 IndexSize = 0;
        } Tables;

        struct TTopics {
            ui64 DataSize = 0;
            ui64 UsedReserveSize = 0;
        } Topics;

        struct TStoragePoolUsage {
            ui64 DataSize = 0;
            ui64 IndexSize = 0;
        };
        THashMap<TString, TStoragePoolUsage> StoragePoolsUsage;
    };

    struct TDiskSpaceQuotas {
        ui64 HardQuota;
        ui64 SoftQuota;

        struct TQuotasPair {
            ui64 HardQuota;
            ui64 SoftQuota;
        };
        THashMap<TString, TQuotasPair> StoragePoolsQuotas;

        explicit operator bool() const {
            return HardQuota || SoftQuota || AnyOf(StoragePoolsQuotas, [](const auto& storagePoolQuota) {
                    return storagePoolQuota.second.HardQuota || storagePoolQuota.second.SoftQuota;
                }
            );
        }
    };

    TSubDomainInfo() = default;
    explicit TSubDomainInfo(ui64 version, const TPathId& resourcesDomainId)
    {
        ProcessingParams.SetVersion(version);
        ResourcesDomainId = resourcesDomainId;
    }

    TSubDomainInfo(ui64 version, ui64 resolution, ui32 bucketsPerMediator, const TPathId& resourcesDomainId)
    {
        ProcessingParams.SetVersion(version);
        ProcessingParams.SetPlanResolution(resolution);
        ProcessingParams.SetTimeCastBucketsPerMediator(bucketsPerMediator);
        ResourcesDomainId = resourcesDomainId;
    }

    TSubDomainInfo(const TSubDomainInfo& other
                   , ui64 planResolution
                   , ui64 timeCastBucketsMediator
                   , TStoragePools additionalPools = {})
        : TSubDomainInfo(other)
    {
        ProcessingParams.SetVersion(other.GetVersion() + 1);

        if (planResolution) {
            Y_ENSURE(other.GetPlanResolution() == 0 || other.GetPlanResolution() == planResolution);
            ProcessingParams.SetPlanResolution(planResolution);
        }

        if (timeCastBucketsMediator) {
            Y_ENSURE(other.GetTCB() == 0 || other.GetTCB() == timeCastBucketsMediator);
            ProcessingParams.SetTimeCastBucketsPerMediator(timeCastBucketsMediator);
        }

        for (auto& toAdd: additionalPools) {
            StoragePools.push_back(toAdd);
        }
    }

    void SetSchemeLimits(const TSchemeLimits& limits, IQuotaCounters* counters = nullptr) {
        SchemeLimits = limits;
        if (counters) {
            counters->SetPathsQuota(limits.MaxPaths);
            counters->SetShardsQuota(limits.MaxShards);
        }
    }

    void MergeSchemeLimits(const NKikimrSubDomains::TSchemeLimits& in, IQuotaCounters* counters = nullptr) {
        SchemeLimits.MergeFromProto(in);
        if (counters) {
            if (in.HasMaxPaths()) {
                counters->SetPathsQuota(SchemeLimits.MaxPaths);
            }
            if (in.HasMaxShards()) {
                counters->SetShardsQuota(SchemeLimits.MaxShards);
            }
        }
    }

    const TSchemeLimits& GetSchemeLimits() const {
        return SchemeLimits;
    }

    ui64 GetVersion() const {
        return ProcessingParams.GetVersion();
    }

    TPtr GetAlter() const {
        return AlterData;
    }

    void SetAlterPrivate(TPtr alterData) {
        AlterData = alterData;
    }

    void SetVersion(ui64 version) {
        Y_ENSURE(ProcessingParams.GetVersion() < version);
        ProcessingParams.SetVersion(version);
    }

    void SetAlter(TPtr alterData) {
        Y_ENSURE(alterData);
        Y_ENSURE(GetVersion() < alterData->GetVersion());
        AlterData = alterData;
    }

    void SetStoragePools(TStoragePools& storagePools, ui64 subDomainVersion) {
        Y_ENSURE(GetVersion() < subDomainVersion);
        StoragePools.swap(storagePools);
        ProcessingParams.SetVersion(subDomainVersion);
    }

    TPathId GetResourcesDomainId() const {
        return ResourcesDomainId;
    }

    void SetResourcesDomainId(const TPathId& domainId) {
        ResourcesDomainId = domainId;
    }

    TTabletId GetSharedHive() const {
        return SharedHive;
    }

    void SetSharedHive(const TTabletId& hiveId) {
        SharedHive = hiveId;
    }

    ui64 GetPlanResolution() const {
        return ProcessingParams.GetPlanResolution();
    }

    ui64 GetTCB() const {
        return ProcessingParams.GetTimeCastBucketsPerMediator();
    }

    TTabletId GetTenantSchemeShardID() const {
        if (!ProcessingParams.HasSchemeShard()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetSchemeShard());
    }

    TTabletId GetTenantHiveID() const {
        if (!ProcessingParams.HasHive()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetHive());
    }

    void SetTenantHiveIDPrivate(const TTabletId& hiveId) {
        ProcessingParams.SetHive(ui64(hiveId));
    }

    TTabletId GetTenantSysViewProcessorID() const {
        if (!ProcessingParams.HasSysViewProcessor()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetSysViewProcessor());
    }

    TTabletId GetTenantStatisticsAggregatorID() const {
        if (!ProcessingParams.HasStatisticsAggregator()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetStatisticsAggregator());
    }

    TTabletId GetTenantBackupControllerID() const {
        if (!ProcessingParams.HasBackupController()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetBackupController());
    }

    TTabletId GetTenantGraphShardID() const {
        if (!ProcessingParams.HasGraphShard()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetGraphShard());
    }

    ui64 GetPathsInside() const {
        return PathsInsideCount;
    }

    void SetPathsInside(ui64 val) {
        PathsInsideCount = val;
    }

    ui64 GetBackupPaths() const {
        return BackupPathsCount;
    }

    ui64 GetSystemPaths() const {
        return SystemPathsCount;
    }

    void IncPathsInside(IQuotaCounters* counters, ui64 delta = 1, EPathCategory category = EPathCategory::Regular) {
        Y_ENSURE(Max<ui64>() - PathsInsideCount >= delta);
        PathsInsideCount += delta;

        switch (category) {
        case EPathCategory::Backup: {
            Y_ENSURE(Max<ui64>() - BackupPathsCount >= delta);
            BackupPathsCount += delta;
            break;
        }
        case EPathCategory::System: {
            Y_ENSURE(Max<ui64>() - SystemPathsCount >= delta);
            SystemPathsCount += delta;
            break;
        }
        case EPathCategory::Regular: {
            counters->ChangePathCount(delta);
            break;
        }
        }
    }

    void DecPathsInside(IQuotaCounters* counters, ui64 delta = 1, EPathCategory category = EPathCategory::Regular) {
        Y_ENSURE(PathsInsideCount >= delta, "PathsInsideCount: " << PathsInsideCount << " delta: " << delta);
        PathsInsideCount -= delta;

        switch (category) {
        case EPathCategory::Backup: {
            Y_ENSURE(BackupPathsCount >= delta, "BackupPathsCount: " << BackupPathsCount << " delta: " << delta);
            BackupPathsCount -= delta;
            break;
        }
        case EPathCategory::System: {
            Y_ENSURE(SystemPathsCount >= delta, "SystemPathsCount: " << SystemPathsCount << " delta: " << delta);
            SystemPathsCount -= delta;
            break;
        }
        case EPathCategory::Regular: {
            counters->ChangePathCount(-delta);
            break;
        }
        }
    }

    ui64 GetPQPartitionsInside() const {
        return PQPartitionsInsideCount;
    }

    void SetPQPartitionsInside(ui64 val) {
        PQPartitionsInsideCount = val;
    }

    void IncPQPartitionsInside(ui64 delta = 1) {
        Y_ENSURE(Max<ui64>() - PQPartitionsInsideCount >= delta);
        PQPartitionsInsideCount += delta;
    }

    void DecPQPartitionsInside(ui64 delta = 1) {
        Y_ENSURE(PQPartitionsInsideCount >= delta, "PQPartitionsInsideCount: " << PQPartitionsInsideCount << " delta: " << delta);
        PQPartitionsInsideCount -= delta;
    }

    ui64 GetPQReservedStorage() const {
        return PQReservedStorage;
    }

    ui64 GetPQAccountStorage() const {
        const auto& topics = DiskSpaceUsage.Topics;
        return topics.DataSize - std::min(topics.UsedReserveSize, PQReservedStorage) + PQReservedStorage;

    }

    void SetPQReservedStorage(ui64 val) {
        PQReservedStorage = val;
    }

    void IncPQReservedStorage(ui64 delta = 1) {
        Y_ENSURE(Max<ui64>() - PQReservedStorage >= delta);
        PQReservedStorage += delta;
    }

    void DecPQReservedStorage(ui64 delta = 1) {
        Y_ENSURE(PQReservedStorage >= delta, "PQReservedStorage: " << PQReservedStorage << " delta: " << delta);
        PQReservedStorage -= delta;
    }

    void UpdatePQReservedStorage(ui64 oldStorage, ui64 newStorage) {
        if (oldStorage == newStorage)
            return;
        DecPQReservedStorage(oldStorage);
        IncPQReservedStorage(newStorage);
    }

    ui64 GetShardsInside() const {
        return InternalShards.size();
    }

    ui64 GetBackupShards() const {
        return BackupShards.size();
    }

    void UpdateCounters(IQuotaCounters* counters);

    void ActualizeAlterData(const THashMap<TShardIdx, TShardInfo>& allShards, TInstant now, bool isExternal, IQuotaCounters* counters) {
        Y_ENSURE(AlterData);

        AlterData->SetPathsInside(GetPathsInside());
        AlterData->InternalShards.swap(InternalShards);
        AlterData->Initialize(allShards);

        AlterData->SchemeQuotas = SchemeQuotas;
        if (isExternal) {
            AlterData->RemoveSchemeQuotas();
        } else if (!AlterData->DeclaredSchemeQuotas && DeclaredSchemeQuotas) {
            AlterData->DeclaredSchemeQuotas = DeclaredSchemeQuotas;
        } else {
            AlterData->RegenerateSchemeQuotas(now);
        }

        if (!AlterData->DatabaseQuotas && DatabaseQuotas && !isExternal) {
            AlterData->DatabaseQuotas = DatabaseQuotas;
        }

        AlterData->DomainStateVersion = DomainStateVersion;
        AlterData->DiskQuotaExceeded = DiskQuotaExceeded;

        // Update DiskSpaceUsage and recheck quotas (which may have changed by an alter)
        AlterData->DiskSpaceUsage = DiskSpaceUsage;
        AlterData->CheckDiskSpaceQuotas(counters);

        CountDiskSpaceQuotas(counters, GetDiskSpaceQuotas(), AlterData->GetDiskSpaceQuotas());
        CountStreamShardsQuota(counters, GetStreamShardsQuota(), AlterData->GetStreamShardsQuota());
        CountStreamReservedStorageQuota(counters, GetStreamReservedStorageQuota(), AlterData->GetStreamReservedStorageQuota());
    }

    ui64 GetStreamShardsQuota() const {
        return DatabaseQuotas ? DatabaseQuotas->data_stream_shards_quota() : 0;
    }

    ui64 GetStreamReservedStorageQuota() const {
        return DatabaseQuotas ? DatabaseQuotas->data_stream_reserved_storage_quota() : 0;
    }

    TDuration GetTtlMinRunInterval() const {
        static constexpr auto TtlMinRunInterval = TDuration::Minutes(15);

        if (!DatabaseQuotas) {
            return TtlMinRunInterval;
        }

        if (!DatabaseQuotas->ttl_min_run_internal_seconds()) {
            return TtlMinRunInterval;
        }

        return TDuration::Seconds(DatabaseQuotas->ttl_min_run_internal_seconds());
    }

    static void CountDiskSpaceQuotas(IQuotaCounters* counters, const TDiskSpaceQuotas& quotas);

    static void CountDiskSpaceQuotas(IQuotaCounters* counters, const TDiskSpaceQuotas& prev, const TDiskSpaceQuotas& next);

    static void CountStreamShardsQuota(IQuotaCounters* counters, const i64 delta) {
        counters->ChangeStreamShardsQuota(delta);
    }

    static void CountStreamReservedStorageQuota(IQuotaCounters* counters, const i64 delta) {
        counters->ChangeStreamReservedStorageQuota(delta);
    }

    static void CountStreamShardsQuota(IQuotaCounters* counters, const i64& prev, const i64& next) {
        counters->ChangeStreamShardsQuota(next - prev);
    }

    static void CountStreamReservedStorageQuota(IQuotaCounters* counters, const i64& prev, const i64& next) {
        counters->ChangeStreamReservedStorageQuota(next - prev);
    }

    TDiskSpaceQuotas GetDiskSpaceQuotas() const;

    /*
    Checks current disk usage against disk quotas.
    Returns true when DiskQuotaExceeded value has changed and needs to be persisted and pushed to scheme board.
    */
    bool CheckDiskSpaceQuotas(IQuotaCounters* counters);

    ui64 TotalDiskSpaceUsage() {
        return DiskSpaceUsage.Tables.TotalSize + (AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota() ? GetPQAccountStorage() : 0);
    }

    ui64 DiskSpaceQuotasAvailable() {
        auto quotas = GetDiskSpaceQuotas();
        if (!quotas) {
            return Max<ui64>();
        }

        auto usage = TotalDiskSpaceUsage();
        return usage < quotas.HardQuota ? quotas.HardQuota - usage : 0;
    }

    const TStoragePools& GetStoragePools() const {
        return StoragePools ;
    }

    const TStoragePools& EffectiveStoragePools() const {
        if (StoragePools) {
            return StoragePools;
        }
        if (AlterData) {
            return AlterData->StoragePools;
        }
        return StoragePools;
    }

    void AddStoragePool(const TStoragePool& pool) {
        StoragePools.push_back(pool);
    }

    void AddPrivateShard(TShardIdx shardId) {
        PrivateShards.push_back(shardId);
    }

    TVector<TShardIdx> GetPrivateShards() const {
        return PrivateShards;
    }

    void AddInternalShard(TShardIdx shardId, IQuotaCounters* counters, bool isBackup = false) {
        InternalShards.insert(shardId);
        if (isBackup) {
            BackupShards.insert(shardId);
        } else {
            counters->ChangeShardCount(1);
        }
    }

    const THashSet<TShardIdx>& GetInternalShards() const {
        return InternalShards;
    }

    void AddInternalShards(const TTxState& txState, IQuotaCounters* counters, bool isBackup = false) {
        for (auto txShard: txState.Shards) {
            if (txShard.Operation != TTxState::CreateParts) {
                continue;
            }
            AddInternalShard(txShard.Idx, counters, isBackup);
        }
    }

    void RemoveInternalShard(TShardIdx shardIdx, IQuotaCounters* counters) {
        auto it = InternalShards.find(shardIdx);
        Y_ENSURE(it != InternalShards.end(), "shardIdx: " << shardIdx);
        InternalShards.erase(it);

        if (BackupShards.contains(shardIdx)) {
            BackupShards.erase(shardIdx);
        } else {
            counters->ChangeShardCount(-1);
        }
    }

    const THashSet<TShardIdx>& GetSequenceShards() const {
        return SequenceShards;
    }

    void AddSequenceShard(const TShardIdx& shardIdx) {
        SequenceShards.insert(shardIdx);
    }

    void RemoveSequenceShard(const TShardIdx& shardIdx) {
        auto it = SequenceShards.find(shardIdx);
        Y_ENSURE(it != SequenceShards.end(), "shardIdx: " << shardIdx);
        SequenceShards.erase(it);
    }

    const NKikimrSubDomains::TProcessingParams& GetProcessingParams() const {
        return ProcessingParams;
    }

    TTabletId GetCoordinator(TTxId txId) const {
        Y_ENSURE(IsSupportTransactions());
        return TTabletId(CoordinatorSelector->Select(ui64(txId)));
    }

    bool IsSupportTransactions() const {
        return !PrivateShards.empty() || (CoordinatorSelector && !CoordinatorSelector->List().empty());
    }

    void Initialize(const THashMap<TShardIdx, TShardInfo>& allShards) {
        if (InitiatedAsGlobal) {
            return;
        }

        ProcessingParams.ClearCoordinators();
        TVector<TTabletId> coordinators = FilterPrivateTablets(ETabletType::Coordinator, allShards);
        for (TTabletId coordinator: coordinators) {
            ProcessingParams.AddCoordinators(ui64(coordinator));
        }
        CoordinatorSelector = new TCoordinators(ProcessingParams);

        ProcessingParams.ClearMediators();
        TVector<TTabletId> mediators = FilterPrivateTablets(ETabletType::Mediator, allShards);
        for (TTabletId mediator: mediators) {
            ProcessingParams.AddMediators(ui64(mediator));
        }

        ProcessingParams.ClearSchemeShard();
        TVector<TTabletId> schemeshards = FilterPrivateTablets(ETabletType::SchemeShard, allShards);
        Y_ENSURE(schemeshards.size() <= 1, "size was: " << schemeshards.size());
        if (schemeshards.size()) {
            ProcessingParams.SetSchemeShard(ui64(schemeshards.front()));
        }

        ProcessingParams.ClearHive();
        TVector<TTabletId> hives = FilterPrivateTablets(ETabletType::Hive, allShards);
        Y_ENSURE(hives.size() <= 1, "size was: " << hives.size());
        if (hives.size()) {
            ProcessingParams.SetHive(ui64(hives.front()));
            SetSharedHive(InvalidTabletId); // set off shared hive when our own hive has found
        }

        ProcessingParams.ClearSysViewProcessor();
        TVector<TTabletId> sysViewProcessors = FilterPrivateTablets(ETabletType::SysViewProcessor, allShards);
        Y_ENSURE(sysViewProcessors.size() <= 1, "size was: " << sysViewProcessors.size());
        if (sysViewProcessors.size()) {
            ProcessingParams.SetSysViewProcessor(ui64(sysViewProcessors.front()));
        }

        ProcessingParams.ClearStatisticsAggregator();
        TVector<TTabletId> statisticsAggregators = FilterPrivateTablets(ETabletType::StatisticsAggregator, allShards);
        Y_ENSURE(statisticsAggregators.size() <= 1, "size was: " << statisticsAggregators.size());
        if (statisticsAggregators.size()) {
            ProcessingParams.SetStatisticsAggregator(ui64(statisticsAggregators.front()));
        }

        ProcessingParams.ClearGraphShard();
        TVector<TTabletId> graphs = FilterPrivateTablets(ETabletType::GraphShard, allShards);
        Y_ENSURE(graphs.size() <= 1, "size was: " << graphs.size());
        if (graphs.size()) {
            ProcessingParams.SetGraphShard(ui64(graphs.front()));
        }
    }

    void InitializeAsGlobal(NKikimrSubDomains::TProcessingParams&& processingParams) {
        InitiatedAsGlobal = true;

        Y_ENSURE(processingParams.GetPlanResolution());
        Y_ENSURE(processingParams.GetTimeCastBucketsPerMediator());

        ui64 version = ProcessingParams.GetVersion();
        ProcessingParams = std::move(processingParams);
        ProcessingParams.SetVersion(version);

        CoordinatorSelector = new TCoordinators(ProcessingParams);
    }

    void AggrDiskSpaceUsage(IQuotaCounters* counters, const TPartitionStats& newAggr, const TPartitionStats& oldAggr = {});
    void AggrDiskSpaceUsage(IQuotaCounters* counters, const TDiskSpaceUsageDelta& delta);

    void AggrDiskSpaceUsage(const TTopicStats& newAggr, const TTopicStats& oldAggr = {});

    const TDiskSpaceUsage& GetDiskSpaceUsage() const {
        return DiskSpaceUsage;
    }

    const TMaybe<NKikimrSubDomains::TSchemeQuotas>& GetDeclaredSchemeQuotas() const {
        return DeclaredSchemeQuotas;
    }

    void SetDeclaredSchemeQuotas(const NKikimrSubDomains::TSchemeQuotas& declaredSchemeQuotas) {
        DeclaredSchemeQuotas.ConstructInPlace(declaredSchemeQuotas);
    }

    const TMaybe<Ydb::Cms::DatabaseQuotas>& GetDatabaseQuotas() const {
        return DatabaseQuotas;
    }

    void SetDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& databaseQuotas) {
        DatabaseQuotas.ConstructInPlace(databaseQuotas);
    }

    void SetDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& databaseQuotas, IQuotaCounters* counters) {
        auto prev = GetDiskSpaceQuotas();
        auto prevs = GetStreamShardsQuota();
        auto prevrs = GetStreamReservedStorageQuota();
        DatabaseQuotas.ConstructInPlace(databaseQuotas);
        auto next = GetDiskSpaceQuotas();
        auto nexts = GetStreamShardsQuota();
        auto nextrs = GetStreamReservedStorageQuota();
        CountDiskSpaceQuotas(counters, prev, next);
        CountStreamShardsQuota(counters, prevs, nexts);
        CountStreamReservedStorageQuota(counters, prevrs, nextrs);
    }

    void ApplyDeclaredSchemeQuotas(const NKikimrSubDomains::TSchemeQuotas& declaredSchemeQuotas, TInstant now) {
        // Check if there was no change in declared quotas
        if (DeclaredSchemeQuotas) {
            TString prev, next;
            Y_ENSURE(DeclaredSchemeQuotas->SerializeToString(&prev));
            Y_ENSURE(declaredSchemeQuotas.SerializeToString(&next));
            if (prev == next) {
                return; // there was no change in quotas
            }
        }

        // Make a local copy of these quotas and regenerate
        DeclaredSchemeQuotas.ConstructInPlace(declaredSchemeQuotas);
        RegenerateSchemeQuotas(now);
    }

    const TSchemeQuotas& GetSchemeQuotas() const {
        return SchemeQuotas;
    }

    void AddSchemeQuota(const TSchemeQuota& quota) {
        SchemeQuotas.emplace_back(quota);
    }

    void AddSchemeQuota(double bucketSize, TDuration bucketDuration, TInstant now) {
        AddSchemeQuota(TSchemeQuota(bucketSize, bucketDuration, now));
    }

    void RemoveSchemeQuotas() {
        SchemeQuotas.LastKnownSize = Max(SchemeQuotas.LastKnownSize, SchemeQuotas.size());
        SchemeQuotas.clear();
    }

    void RegenerateSchemeQuotas(TInstant now) {
        RemoveSchemeQuotas();
        if (DeclaredSchemeQuotas) {
            for (const auto& declaredQuota : DeclaredSchemeQuotas->GetSchemeQuotas()) {
                double bucketSize = declaredQuota.GetBucketSize();
                TDuration bucketDuration = TDuration::Seconds(declaredQuota.GetBucketSeconds());
                SchemeQuotas.emplace_back(bucketSize, bucketDuration, now);
            }
        }
    }

    bool TryConsumeSchemeQuota(TInstant now) {
        bool ok = true;
        for (auto& quota : SchemeQuotas) {
            quota.Update(now);
            ok &= quota.CanPush(1.0);
        }

        if (!ok) {
            return false;
        }

        for (auto& quota : SchemeQuotas) {
            quota.Push(now, 1.0);
        }

        return true;
    }

    ui64 GetDomainStateVersion() const {
        return DomainStateVersion;
    }

    void SetDomainStateVersion(ui64 version) {
        DomainStateVersion = version;
    }

    bool GetDiskQuotaExceeded() const {
        return DiskQuotaExceeded;
    }

    void SetDiskQuotaExceeded(bool value) {
        DiskQuotaExceeded = value;
    }

    const NLoginProto::TSecurityState& GetSecurityState() const {
        return SecurityState;
    }

    void UpdateSecurityState(NLoginProto::TSecurityState state) {
        SecurityState = std::move(state);
    }

    ui64 GetSecurityStateVersion() const {
        return SecurityStateVersion;
    }

    void SetSecurityStateVersion(ui64 securityStateVersion) {
        SecurityStateVersion = securityStateVersion;
    }

    void IncSecurityStateVersion() {
        ++SecurityStateVersion;
    }

    using TMaybeAuditSettings = TMaybe<NKikimrSubDomains::TAuditSettings, NMaybe::TPolicyUndefinedFail>;

    void SetAuditSettings(const NKikimrSubDomains::TAuditSettings& value) {
        AuditSettings.ConstructInPlace(value);
    }

    const TMaybeAuditSettings& GetAuditSettings() const {
        return AuditSettings;
    }

    void ApplyAuditSettings(const TMaybeAuditSettings& diff);

    const TMaybeServerlessComputeResourcesMode& GetServerlessComputeResourcesMode() const {
        return ServerlessComputeResourcesMode;
    }

    void SetServerlessComputeResourcesMode(EServerlessComputeResourcesMode serverlessComputeResourcesMode) {
        Y_ENSURE(serverlessComputeResourcesMode, "Can't set ServerlessComputeResourcesMode to unspecified");
        ServerlessComputeResourcesMode = serverlessComputeResourcesMode;
    }

private:
    bool InitiatedAsGlobal = false;
    NKikimrSubDomains::TProcessingParams ProcessingParams;
    TCoordinators::TPtr CoordinatorSelector;
    TMaybe<NKikimrSubDomains::TSchemeQuotas> DeclaredSchemeQuotas;
    TMaybe<Ydb::Cms::DatabaseQuotas> DatabaseQuotas;
    ui64 DomainStateVersion = 0;
    bool DiskQuotaExceeded = false;

    TVector<TShardIdx> PrivateShards;
    TStoragePools StoragePools;
    TPtr AlterData;

    TSchemeLimits SchemeLimits;
    TSchemeQuotas SchemeQuotas;

    ui64 PathsInsideCount = 0;
    ui64 BackupPathsCount = 0;
    ui64 SystemPathsCount = 0;
    TDiskSpaceUsage DiskSpaceUsage;

    THashSet<TShardIdx> InternalShards;
    THashSet<TShardIdx> BackupShards;
    THashSet<TShardIdx> SequenceShards;

    ui64 PQPartitionsInsideCount = 0;
    ui64 PQReservedStorage = 0;

    TPathId ResourcesDomainId;
    TTabletId SharedHive = InvalidTabletId;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;

    NLoginProto::TSecurityState SecurityState;
    ui64 SecurityStateVersion = 0;

    TMaybeAuditSettings AuditSettings;

    TVector<TTabletId> FilterPrivateTablets(TTabletTypes::EType type, const THashMap<TShardIdx, TShardInfo>& allShards) const {
        TVector<TTabletId> tablets;
        for (auto shardId: PrivateShards) {

            if (!allShards.contains(shardId)) {
                // KIKIMR-9849
                // some private shards, which has been migrated, might be deleted
                continue;
            }

            const auto& shard = allShards.at(shardId);
            if (shard.TabletType == type && shard.TabletID != InvalidTabletId) {
                tablets.push_back(shard.TabletID);
            }
        }
        return tablets;
    }
};

struct TBlockStorePartitionInfo : public TSimpleRefCount<TBlockStorePartitionInfo> {
    using TPtr = TIntrusivePtr<TBlockStorePartitionInfo>;

    ui32 PartitionId = 0;
    ui64 AlterVersion = 0;
};

struct TBlockStoreVolumeInfo : public TSimpleRefCount<TBlockStoreVolumeInfo> {
    using TPtr = TIntrusivePtr<TBlockStoreVolumeInfo>;

    struct TTabletCache {
        ui64 AlterVersion = 0;
        TVector<TTabletId> Tablets;
    };

    static constexpr size_t NumVolumeTabletChannels = 3;

    ui32 DefaultPartitionCount = 0;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    ui64 AlterVersion = 0;
    ui64 TokenVersion = 0;
    THashMap<TShardIdx, TBlockStorePartitionInfo::TPtr> Shards; // key ShardIdx
    TBlockStoreVolumeInfo::TPtr AlterData;
    TTabletId VolumeTabletId = InvalidTabletId;
    TShardIdx VolumeShardIdx = InvalidShardIdx;
    TString MountToken;
    TTabletCache TabletCache;
    ui32 ExplicitChannelProfileCount = 0;

    static ui32 CalculateDefaultPartitionCount(
        const NKikimrBlockStore::TVolumeConfig& config)
    {
        ui32 c = 0;
        for (const auto& partition: config.GetPartitions()) {
            if (partition.GetType() == NKikimrBlockStore::EPartitionType::Default) {
                ++c;
            }
        }

        return c;
    }

    bool HasVolumeTablet() const { return VolumeTabletId != InvalidTabletId; }

    void PrepareAlter(TBlockStoreVolumeInfo::TPtr alterData) {
        Y_ENSURE(alterData, "No alter data at Alter preparation");
        if (!alterData->DefaultPartitionCount) {
            alterData->DefaultPartitionCount =
                CalculateDefaultPartitionCount(alterData->VolumeConfig);
        }
        alterData->VolumeTabletId = VolumeTabletId;
        alterData->VolumeShardIdx = VolumeShardIdx;
        alterData->AlterVersion = AlterVersion + 1;
        AlterData = alterData;
    }

    void ForgetAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter rollback");
        AlterData.Reset();
    }

    void FinishAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter completion");
        DefaultPartitionCount = AlterData->DefaultPartitionCount;
        ExplicitChannelProfileCount = AlterData->ExplicitChannelProfileCount;
        VolumeConfig.CopyFrom(AlterData->VolumeConfig);
        ++AlterVersion;
        Y_ENSURE(AlterVersion == AlterData->AlterVersion);
        Y_ENSURE(VolumeTabletId == AlterData->VolumeTabletId || !HasVolumeTablet());
        Y_ENSURE(AlterData->HasVolumeTablet());
        Y_ENSURE(AlterData->VolumeShardIdx);
        VolumeTabletId = AlterData->VolumeTabletId;
        VolumeShardIdx = AlterData->VolumeShardIdx;
        AlterData.Reset();
    }

    const TVector<TTabletId>& GetTablets(const THashMap<TShardIdx, TShardInfo>& allShards) {
        if (TabletCache.AlterVersion == AlterVersion) {
            return TabletCache.Tablets;
        }

        TabletCache.Tablets.clear();
        TabletCache.Tablets.resize(DefaultPartitionCount);

        for (const auto& kv : Shards) {
            TShardIdx shardIdx = kv.first;
            const auto& partInfo = *kv.second;

            auto itShard = allShards.find(shardIdx);
            Y_ENSURE(itShard != allShards.end(), "No shard with shardIdx " << shardIdx);
            TTabletId tabletId = itShard->second.TabletID;

            if (partInfo.AlterVersion <= AlterVersion) {
                Y_ENSURE(partInfo.PartitionId < DefaultPartitionCount,
                    "Wrong PartitionId " << partInfo.PartitionId);
                TabletCache.Tablets[partInfo.PartitionId] = tabletId;
            }
        }

        // Verify there are no missing tabletIds
        for (ui32 idx = 0; idx < TabletCache.Tablets.size(); ++idx) {
            TTabletId tabletId = TabletCache.Tablets[idx];
            Y_ENSURE(tabletId, "Unassigned tabletId"
                           << " for partition " << idx
                           << " out of " << TabletCache.Tablets.size()
                           << " TabletCache.AlterVersion" << TabletCache.AlterVersion
                           << " AlterVersion " << AlterVersion);
        }

        TabletCache.AlterVersion = AlterVersion;
        return TabletCache.Tablets;
    }

    TVolumeSpace GetVolumeSpace() const {
        ui64 blockSize = VolumeConfig.GetBlockSize();
        ui64 blockCount = 0;
        for (const auto& partition: VolumeConfig.GetPartitions()) {
            blockCount += partition.GetBlockCount();
        }

        TVolumeSpace space;
        space.Raw += blockCount * blockSize;
        switch (VolumeConfig.GetStorageMediaKind()) {
            case 1: // STORAGE_MEDIA_SSD
                if (VolumeConfig.GetIsSystem()) {
                    space.SSDSystem += blockCount * blockSize; // merged blobs
                } else {
                    space.SSD += blockCount * blockSize; // merged blobs
                    space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                }
                break;
            case 2: // STORAGE_MEDIA_HYBRID
                space.HDD += blockCount * blockSize; // merged blobs
                space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                break;
            case 3: // STORAGE_MEDIA_HDD
                space.HDD += blockCount * blockSize; // merged blobs
                space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                break;
            case 4: // STORAGE_MEDIA_SSD_NONREPLICATED
                space.SSDNonrepl += blockCount * blockSize; // blocks are stored directly
                break;
        }

        if (AlterData) {
            auto altSpace = AlterData->GetVolumeSpace();
            space.Raw = Max(space.Raw, altSpace.Raw);
            space.SSD = Max(space.SSD, altSpace.SSD);
            space.HDD = Max(space.HDD, altSpace.HDD);
            space.SSDNonrepl = Max(space.SSDNonrepl, altSpace.SSDNonrepl);
            space.SSDSystem = Max(space.SSDSystem, altSpace.SSDSystem);
        }

        return space;
    }
};

struct TFileStoreInfo : public TSimpleRefCount<TFileStoreInfo> {
    using TPtr = TIntrusivePtr<TFileStoreInfo>;

    TShardIdx IndexShardIdx = InvalidShardIdx;
    TTabletId IndexTabletId = InvalidTabletId;

    NKikimrFileStore::TConfig Config;
    ui64 Version = 0;

    THolder<NKikimrFileStore::TConfig> AlterConfig;
    ui64 AlterVersion = 0;

    void PrepareAlter(const NKikimrFileStore::TConfig& alterConfig) {
        Y_ENSURE(!AlterConfig);
        Y_ENSURE(!AlterVersion);

        AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
        AlterConfig->CopyFrom(alterConfig);

        Y_ENSURE(!AlterConfig->GetBlockSize());
        AlterConfig->SetBlockSize(Config.GetBlockSize());

        AlterVersion = Version + 1;
    }

    void ForgetAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        AlterConfig.Reset();
        AlterVersion = 0;
    }

    void FinishAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);

        ForgetAlter();
    }

    TFileStoreSpace GetFileStoreSpace() const {
        auto space = GetFileStoreSpace(Config);

        if (AlterConfig) {
            const auto alterSpace = GetFileStoreSpace(*AlterConfig);
            space.SSD = Max(space.SSD, alterSpace.SSD);
            space.HDD = Max(space.HDD, alterSpace.HDD);
            space.SSDSystem = Max(space.SSDSystem, alterSpace.SSDSystem);
        }

        return space;
    }

private:
    TFileStoreSpace GetFileStoreSpace(const NKikimrFileStore::TConfig& config) const {
        const ui64 blockSize = config.GetBlockSize();
        const ui64 blockCount = config.GetBlocksCount();

        TFileStoreSpace space;
        switch (config.GetStorageMediaKind()) {
            case 1: // STORAGE_MEDIA_SSD
                if (config.GetIsSystem()) {
                    space.SSDSystem += blockCount * blockSize;
                } else {
                    space.SSD += blockCount * blockSize;
                }
                break;
            case 2: // STORAGE_MEDIA_HYBRID
            case 3: // STORAGE_MEDIA_HDD
                space.HDD += blockCount * blockSize;
                break;
        }

        return space;
    }
};

struct TKesusInfo : public TSimpleRefCount<TKesusInfo> {
    using TPtr = TIntrusivePtr<TKesusInfo>;

    TShardIdx KesusShardIdx = InvalidShardIdx;
    TTabletId KesusTabletId = InvalidTabletId;
    Ydb::Coordination::Config Config;
    ui64 Version = 0;
    THolder<Ydb::Coordination::Config> AlterConfig;
    ui64 AlterVersion = 0;

    void FinishAlter() {
        Y_ENSURE(AlterConfig, "No alter config at Alter completion");
        Y_ENSURE(AlterVersion, "No alter version at Alter completion");
        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);
        AlterConfig.Reset();
        AlterVersion = 0;
    }
};

struct TTableIndexInfo : public TSimpleRefCount<TTableIndexInfo> {
    using TPtr = TIntrusivePtr<TTableIndexInfo>;
    using EType = NKikimrSchemeOp::EIndexType;
    using EState = NKikimrSchemeOp::EIndexState;

    TTableIndexInfo(ui64 version, EType type, EState state, std::string_view description)
        : AlterVersion(version)
        , Type(type)
        , State(state)
    {
        switch (type) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
                // no specialized index description
                Y_ASSERT(description.empty());
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TFulltextIndexDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            default:
                Y_DEBUG_ABORT_S(NTableIndex::InvalidIndexType(type));
                break;
        }
    }

    TTableIndexInfo(const TTableIndexInfo&) = default;

    TPtr CreateNextVersion() {
        this->AlterData = this->GetNextVersion();
        return this->AlterData;
    }

    TPtr GetNextVersion() const {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TTableIndexInfo(*this);
        ++result->AlterVersion;
        return result;
    }

    TString SerializeDescription() const {
        return std::visit([]<typename T>(const T& v) {
            if constexpr (std::is_same_v<std::monostate, T>) {
                return TString{};
            } else {
                TString str{v.SerializeAsString()};
                Y_ENSURE(!str.empty());
                return str;
            }
        }, SpecializedIndexDescription);
    }

    static TPtr NotExistedYet(EType type) {
        return new TTableIndexInfo(0, type, EState::EIndexStateInvalid, {});
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexCreationConfig& config, TString& errMsg) {
        if (!config.KeyColumnNamesSize()) {
            errMsg += TStringBuilder() << "no key columns in index creation config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        Y_ENSURE(!alterData->IndexKeys.empty());
        alterData->IndexDataColumns.assign(config.GetDataColumnNames().begin(), config.GetDataColumnNames().end());

        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        switch (GetIndexType(config)) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
                // no specialized index description
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
                alterData->SpecializedIndexDescription = config.GetVectorIndexKmeansTreeDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
                alterData->SpecializedIndexDescription = config.GetFulltextIndexDescription();
                break;
            default:
                errMsg += InvalidIndexType(config.GetType());
                return nullptr;
        }

        return result;
    }

    ui64 AlterVersion = 1;
    EType Type;
    EState State;

    TVector<TString> IndexKeys;
    TVector<TString> IndexDataColumns;

    TTableIndexInfo::TPtr AlterData = nullptr;

    std::variant<std::monostate,
        NKikimrSchemeOp::TVectorIndexKmeansTreeDescription,
        NKikimrSchemeOp::TFulltextIndexDescription> SpecializedIndexDescription;
};

struct TCdcStreamSettings {
    using TSelf = TCdcStreamSettings;
    using EMode = NKikimrSchemeOp::ECdcStreamMode;
    using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
    using EState = NKikimrSchemeOp::ECdcStreamState;

    #define OPTION(type, name) \
        TSelf&& With##name(type value) && { \
            name = std::move(value); \
            return std::move(*this); \
        } \
        type name;

    OPTION(EMode, Mode);
    OPTION(EFormat, Format);
    OPTION(bool, VirtualTimestamps);
    OPTION(TDuration, ResolvedTimestamps);
    OPTION(bool, SchemaChanges);
    OPTION(TString, AwsRegion);
    OPTION(EState, State);

    #undef OPTION
};

struct TCdcStreamInfo
    : public TCdcStreamSettings
    , public TSimpleRefCount<TCdcStreamInfo>
{
    using TPtr = TIntrusivePtr<TCdcStreamInfo>;

    // shards of the table
    struct TShardStatus {
        NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus Status;

        explicit TShardStatus(NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status)
            : Status(status)
        {}
    };

    TCdcStreamInfo(ui64 version, TCdcStreamSettings&& settings)
        : TCdcStreamSettings(std::move(settings))
        , AlterVersion(version)
    {}

    TCdcStreamInfo(const TCdcStreamInfo&) = default;

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TCdcStreamInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static TPtr New(TCdcStreamSettings settings) {
        settings.State = EState::ECdcStreamStateInvalid;
        return new TCdcStreamInfo(0, std::move(settings));
    }

    static TPtr Create(const NKikimrSchemeOp::TCdcStreamDescription& desc) {
        TPtr result = New(TCdcStreamSettings()
            .WithMode(desc.GetMode())
            .WithFormat(desc.GetFormat())
            .WithVirtualTimestamps(desc.GetVirtualTimestamps())
            .WithResolvedTimestamps(TDuration::MilliSeconds(desc.GetResolvedTimestampsIntervalMs()))
            .WithSchemaChanges(desc.GetSchemaChanges())
            .WithAwsRegion(desc.GetAwsRegion()));
        TPtr alterData = result->CreateNextVersion();
        alterData->State = EState::ECdcStreamStateReady;
        if (desc.HasState()) {
            alterData->State = desc.GetState();
        }

        return result;
    }

    void Serialize(NKikimrSchemeOp::TCdcStreamDescription& desc) const {
        desc.SetSchemaVersion(AlterVersion);
        desc.SetMode(Mode);
        desc.SetFormat(Format);
        desc.SetVirtualTimestamps(VirtualTimestamps);
        desc.SetResolvedTimestampsIntervalMs(ResolvedTimestamps.MilliSeconds());
        desc.SetSchemaChanges(SchemaChanges);
        desc.SetAwsRegion(AwsRegion);
        desc.SetState(State);
        if (ScanShards) {
            auto& scanProgress = *desc.MutableScanProgress();
            scanProgress.SetShardsTotal(ScanShards.size());
            scanProgress.SetShardsCompleted(DoneShards.size());
        }
    }

    void FinishAlter() {
        Y_ENSURE(AlterData);

        AlterVersion = AlterData->AlterVersion;
        static_cast<TCdcStreamSettings&>(*this) = static_cast<TCdcStreamSettings&>(*AlterData);

        AlterData.Reset();
    }

    ui64 AlterVersion = 1;
    TCdcStreamInfo::TPtr AlterData = nullptr;

    TMap<TShardIdx, TShardStatus> ScanShards;
    THashSet<TShardIdx> PendingShards;
    THashSet<TShardIdx> InProgressShards;
    THashSet<TShardIdx> DoneShards;
};

struct TSequenceInfo : public TSimpleRefCount<TSequenceInfo> {
    using TPtr = TIntrusivePtr<TSequenceInfo>;

    explicit TSequenceInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    { }

    TSequenceInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TSequenceDescription&& description,
        NKikimrSchemeOp::TSequenceSharding&& sharding);

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TSequenceInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static bool ValidateCreate(const NKikimrSchemeOp::TSequenceDescription& p, TString& err);

    ui64 AlterVersion = 0;
    TSequenceInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TSequenceDescription Description;
    NKikimrSchemeOp::TSequenceSharding Sharding;

    ui64 SequenceShard = 0;
};

struct TReplicationInfo : public TSimpleRefCount<TReplicationInfo> {
    using TPtr = TIntrusivePtr<TReplicationInfo>;

    TReplicationInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {
    }

    TReplicationInfo(ui64 alterVersion, NKikimrSchemeOp::TReplicationDescription&& desc)
        : AlterVersion(alterVersion)
        , Description(std::move(desc))
    {
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);

        TPtr result = new TReplicationInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;

        return result;
    }

    static TPtr New() {
        return new TReplicationInfo(0);
    }

    static TPtr Create(NKikimrSchemeOp::TReplicationDescription&& desc) {
        TPtr result = New();
        TPtr alterData = result->CreateNextVersion();
        alterData->Description = std::move(desc);

        return result;
    }

    ui64 AlterVersion = 0;
    TReplicationInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TReplicationDescription Description;
    TShardIdx ControllerShardIdx = InvalidShardIdx;
};

struct TBlobDepotInfo : TSimpleRefCount<TBlobDepotInfo> {
    using TPtr = TIntrusivePtr<TBlobDepotInfo>;

    TBlobDepotInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {}

    TBlobDepotInfo(ui64 alterVersion, const NKikimrSchemeOp::TBlobDepotDescription& desc)
        : AlterVersion(alterVersion)
    {
        Description.CopyFrom(desc);
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(!AlterData);
        AlterData = MakeIntrusive<TBlobDepotInfo>(*this);
        ++AlterData->AlterVersion;
        return AlterData;
    }

    ui64 AlterVersion = 0;
    TPtr AlterData = nullptr;
    TShardIdx BlobDepotShardIdx = InvalidShardIdx;
    TTabletId BlobDepotTabletId = InvalidTabletId;
    NKikimrSchemeOp::TBlobDepotDescription Description;
};

struct TPublicationInfo {
    TSet<std::pair<TPathId, ui64>> Paths;
    THashSet<TActorId> Subscribers;
};

// namespace NExport {
struct TExportInfo: public TSimpleRefCount<TExportInfo> {
    using TPtr = TIntrusivePtr<TExportInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Waiting = 1,
        CreateExportDir = 2,
        CopyTables = 3,
        Transferring = 4,
        UploadExportMetadata = 5,
        Done = 240,
        Dropping = 241,
        Dropped = 242,
        AutoDropping = 243,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        YT = 0,
        S3,
        FS,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        TString SourcePathName;
        TPathId SourcePathId;
        NKikimrSchemeOp::EPathType SourcePathType;
        ui32 ParentIdx; // used by indexes

        EState State = EState::Waiting;
        ESubState SubState = ESubState::AllocateTxId;
        TTxId WaitTxId = InvalidTxId;
        TActorId SchemeUploader;
        TString Issue;

        TItem() = default;

        explicit TItem(
                const TString& sourcePathName,
                const TPathId sourcePathId,
                NKikimrSchemeOp::EPathType sourcePathType,
                ui32 parentIdx = Max<ui32>())
            : SourcePathName(sourcePathName)
            , SourcePathId(sourcePathId)
            , SourcePathType(sourcePathType)
            , ParentIdx(parentIdx)
        {
        }

        TString ToString(ui32 idx) const;

        static bool IsDone(const TItem& item);
        static bool IsDropped(const TItem& item);
    };

    ui64 Id;  // TxId from the original TEvCreateExportRequest
    TString Uid;
    EKind Kind;
    TString Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;
    TString PeerName;  // required for making audit log records
    TString SanitizedToken;  // required for making audit log records

    TVector<TItem> Items;

    TPathId ExportPathId = InvalidPathId;
    EState State = EState::Invalid;
    TTxId WaitTxId = InvalidTxId;
    THashSet<TTxId> DependencyTxIds; // volatile set of concurrent tx(s)
    TString Issue;

    TDeque<ui32> PendingItems;
    TDeque<ui32> PendingDropItems;

    TSet<TActorId> Subscribers;

    ui64 SnapshotStep = 0;
    ui64 SnapshotTxId = 0;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    bool EnableChecksums = false;
    bool EnablePermissions = false;
    bool IncludeIndexData = false;

    NKikimrSchemeOp::TExportMetadata ExportMetadata;
    TActorId ExportMetadataUploader;

    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TString& settings,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , Settings(settings)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
    }

    template <typename TSettingsPB>
    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TSettingsPB& settingsPb,
            const TPathId domainPathId,
            const TString& peerName)
        : TExportInfo(id, uid, kind, SerializeSettings(settingsPb), domainPathId, peerName)
    {
    }

    bool IsValid() const {
        return State != EState::Invalid;
    }

    bool IsPreparing() const {
        return State == EState::CreateExportDir || State == EState::CopyTables || State == EState::UploadExportMetadata;
    }

    bool IsWorking() const {
        return State == EState::Transferring;
    }

    bool IsDropping() const {
        return State == EState::Dropping;
    }

    bool IsAutoDropping() const {
        return State == EState::AutoDropping;
    }

    bool IsCancelling() const {
        return State == EState::Cancellation;
    }

    bool IsInProgress() const {
        return IsPreparing() || IsWorking() || IsDropping() || IsAutoDropping() || IsCancelling();
    }

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool AllItemsAreDropped() const;
    void AddNotifySubscriber(const TActorId& actorId);

    TString ToString() const;
}; // TExportInfo
// } // NExport

// namespace NImport {
struct TImportInfo: public TSimpleRefCount<TImportInfo> {
    using TPtr = TIntrusivePtr<TImportInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Waiting = 1,
        GetScheme = 2,
        CreateSchemeObject = 3,
        Transferring = 4,
        BuildIndexes = 5,
        CreateChangefeed = 6,
        DownloadExportMetadata = 7,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        S3 = 0,
        FS = 1,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        enum class EChangefeedState: ui8 {
            CreateChangefeed = 0,
            CreateConsumers,
        };

        TString DstPathName;
        TPathId DstPathId;
        TString SrcPrefix;
        TString SrcPath; // Src path from schema mapping
        TMaybe<Ydb::Table::CreateTableRequest> Table;
        TMaybe<Ydb::Topic::CreateTopicRequest> Topic;
        TMaybe<Ydb::Table::DescribeSystemViewResult> SysView;
        TString CreationQuery;
        TMaybe<NKikimrSchemeOp::TModifyScheme> PreparedCreationQuery;
        TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest> Permissions;
        NBackup::TMetadata Metadata;
        TVector<std::pair<NBackup::TIndexMetadata, Ydb::Table::CreateTableRequest>> MaterializedIndexes;
        NKikimrSchemeOp::TImportTableChangefeeds Changefeeds;

        EState State = EState::GetScheme;
        ESubState SubState = ESubState::AllocateTxId;
        EChangefeedState ChangefeedState = EChangefeedState::CreateChangefeed;
        TTxId WaitTxId = InvalidTxId;
        TActorId SchemeGetter;
        TActorId SchemeQueryExecutor;
        int NextIndexIdx = 0;
        int NextChangefeedIdx = 0;
        TString Issue;
        TPathId StreamImplPathId;
        TMaybe<NBackup::TEncryptionIV> ExportItemIV;

        ui32 ParentIdx = Max<ui32>();
        TVector<ui32> ChildItems;

        TItem() = default;

        explicit TItem(const TString& dstPathName)
            : DstPathName(dstPathName)
        {
        }

        explicit TItem(const TString& dstPathName, const TPathId& dstPathId)
            : DstPathName(dstPathName)
            , DstPathId(dstPathId)
        {
        }

        TString ToString(ui32 idx) const;

        static bool IsDone(const TItem& item);
    };

    ui64 Id;  // TxId from the original TEvCreateImportRequest
    TString Uid;
    EKind Kind;
    TString SettingsSerialized;
    std::variant<Ydb::Import::ImportFromS3Settings,
                 Ydb::Import::ImportFromFsSettings> Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;
    TString PeerName;  // required for making audit log records
    TString SanitizedToken;  // required for making audit log records
    TMaybe<NBackup::TEncryptionIV> ExportIV;
    TMaybe<NBackup::TSchemaMapping> SchemaMapping;
    TActorId SchemaMappingGetter;

    EState State = EState::Invalid;
    TString Issue;
    TVector<TItem> Items;
    int WaitingSchemeObjects = 0;

    TSet<TActorId> Subscribers;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    TMaybe<std::vector<TRegExMatch>> ExcludeRegexps;

private:
    template <typename TSettingsPB>
    static TString SerializeSettings(const TSettingsPB& settings) {
        TString serialized;
        Y_ABORT_UNLESS(settings.SerializeToString(&serialized));
        return serialized;
    }

    template <typename TFunc>
    auto Visit(TFunc&& func) const {
        return VisitSettings(Settings, std::forward<TFunc>(func));
    }

public:

    TString GetItemSrcPrefix(size_t i) const {
        if (i < Items.size() && Items[i].SrcPrefix) {
            return Items[i].SrcPrefix;
        }

        // Backward compatibility.
        // But there can be no paths in settings at all.
        return Visit([i](const auto& settings) -> TString {
            return GetItemSource(settings, i);
        });
    }

    const Ydb::Import::ImportFromS3Settings& GetS3Settings() const {
        Y_ABORT_UNLESS(Kind == EKind::S3);
        return std::get<Ydb::Import::ImportFromS3Settings>(Settings);
    }

    const Ydb::Import::ImportFromFsSettings& GetFsSettings() const {
        Y_ABORT_UNLESS(Kind == EKind::FS);
        return std::get<Ydb::Import::ImportFromFsSettings>(Settings);
    }

    // Getters for common settings fields
    bool GetNoAcl() const {
        return Visit([](const auto& settings) {
            return settings.no_acl();
        });
    }

    bool GetSkipChecksumValidation() const {
        return Visit([](const auto& settings) {
            return settings.skip_checksum_validation();
        });
    }

    bool CompileExcludeRegexps(TString& errorDescription);

    bool IsExcludedFromImport(const TString& path) const;

    explicit TImportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TString& serializedSettings,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , SettingsSerialized(serializedSettings)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
        switch (kind) {
        case EKind::S3: {
            Settings = ParseSettings<Ydb::Import::ImportFromS3Settings>(serializedSettings);
            break;
        }
        case EKind::FS: {
            Settings = ParseSettings<Ydb::Import::ImportFromFsSettings>(serializedSettings);
            break;
        }
        default:
            Y_ABORT("Unknown import kind");
        }
    }

    template <typename TSettingsPB>
    explicit TImportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TSettingsPB& settingsPb,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , SettingsSerialized(SerializeSettings(settingsPb))
        , Settings(settingsPb)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
    }

public:

    TString ToString() const;

    bool IsFinished() const;

    void AddNotifySubscriber(const TActorId& actorId);

    struct TFillItemsFromSchemaMappingResult {
        bool Success = true;
        TString ErrorMessage;
        size_t ErrorsCount = 0;

        void AddError(const TString& err);
    };

    // Erases encryption key and syncronize it with SettingsSerialized
    // Returns true if settings changed
    bool EraseEncryptionKey() {
        return std::visit([this](auto& settings) {
            if (settings.encryption_settings().has_symmetric_key()) {
                settings.mutable_encryption_settings()->clear_symmetric_key();
                Y_ABORT_UNLESS(settings.SerializeToString(&SettingsSerialized));
                return true;
            }
            return false;
        }, Settings);
    }

    // Fills items from schema mapping:
    // - if user specified no items, fills all from schema mapping;
    // - if user specified explicit filtering, takes from schema mapping only those allowed by filter.
    //
    // Replaces current items list with a new list of items.
    // Generates an error if there are no item explicitly specified by filter.
    TFillItemsFromSchemaMappingResult FillItemsFromSchemaMapping(TSchemeShard* ss);
}; // TImportInfo
// } // NImport

struct TExternalTableInfo: TSimpleRefCount<TExternalTableInfo> {
    using TPtr = TIntrusivePtr<TExternalTableInfo>;

    TString SourceType;
    TString DataSourcePath;
    TString Location;
    ui64 AlterVersion = 0;
    THashMap<ui32, TTableInfo::TColumn> Columns;
    TString Content;
};

struct TExternalDataSourceInfo: TSimpleRefCount<TExternalDataSourceInfo> {
    using TPtr = TIntrusivePtr<TExternalDataSourceInfo>;

    ui64 AlterVersion = 0;
    TString SourceType;
    TString Location;
    TString Installation;
    NKikimrSchemeOp::TAuth Auth;
    NKikimrSchemeOp::TExternalTableReferences ExternalTableReferences;
    NKikimrSchemeOp::TExternalDataSourceProperties Properties;

    void FillProto(NKikimrSchemeOp::TExternalDataSourceDescription& proto, bool withReferences = true) const {
        proto.SetVersion(AlterVersion);
        proto.SetSourceType(SourceType);
        proto.SetLocation(Location);
        proto.SetInstallation(Installation);
        proto.MutableAuth()->CopyFrom(Auth);
        proto.MutableProperties()->CopyFrom(Properties);
        if (withReferences) {
            proto.MutableReferences()->CopyFrom(ExternalTableReferences);
        }
    }
};

struct TViewInfo : TSimpleRefCount<TViewInfo> {
    using TPtr = TIntrusivePtr<TViewInfo>;

    ui64 AlterVersion = 0;
    TString QueryText;
    NYql::NProto::TTranslationSettings CapturedContext;
};

struct TResourcePoolInfo : TSimpleRefCount<TResourcePoolInfo> {
    using TPtr = TIntrusivePtr<TResourcePoolInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TResourcePoolProperties Properties;
};

struct TBackupCollectionInfo : TSimpleRefCount<TBackupCollectionInfo> {
    using TPtr = TIntrusivePtr<TBackupCollectionInfo>;

    static TPtr New() {
        return new TBackupCollectionInfo();
    }

    static TPtr Create(const NKikimrSchemeOp::TBackupCollectionDescription& desc) {
        TPtr result = New();

        result->Description = desc;

        return result;
    }

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TBackupCollectionDescription Description;
};

struct TSysViewInfo : TSimpleRefCount<TSysViewInfo> {
    using TPtr = TIntrusivePtr<TSysViewInfo>;

    ui64 AlterVersion = 0;
    NKikimrSysView::ESysViewType Type;
};

struct TIncrementalRestoreState {
    enum class EState : ui32 {
        Running = 1,
        Finalizing = 2,
        Completed = 3,
    };

    EState State = EState::Running;

    // The backup collection path this restore belongs to
    TPathId BackupCollectionPathId; // used for DB scoping and finalization

    // Global id of the original incremental restore operation
    ui64 OriginalOperationId = 0;

    // Sequential incremental backup processing
    struct TIncrementalBackup {
        TPathId BackupPathId;
        TString BackupPath;
        ui64 Timestamp;
        bool Completed = false;

        TIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp)
            : BackupPathId(pathId), BackupPath(path), Timestamp(timestamp)
        {}
    };

    // Table operation state for tracking DataShard completion
    struct TTableOperationState {
        TOperationId OperationId;
        THashSet<TShardIdx> ExpectedShards;
        THashSet<TShardIdx> CompletedShards;
        THashSet<TShardIdx> FailedShards;

        TTableOperationState() = default;
        explicit TTableOperationState(const TOperationId& opId) : OperationId(opId) {}

        bool AllShardsComplete() const {
            return CompletedShards.size() + FailedShards.size() == ExpectedShards.size() &&
                    !ExpectedShards.empty();
        }

        bool HasFailures() const {
            return !FailedShards.empty();
        }
    };

    TVector<TIncrementalBackup> IncrementalBackups; // Sorted by timestamp
    ui32 CurrentIncrementalIdx = 0;
    bool CurrentIncrementalStarted = false;

    // Operation completion tracking for current incremental backup
    THashSet<TOperationId> InProgressOperations;
    THashSet<TOperationId> CompletedOperations;

    // Table operation state tracking for DataShard completion
    THashMap<TOperationId, TTableOperationState> TableOperations;

    THashSet<TShardIdx> InvolvedShards;

    bool AllIncrementsProcessed() const {
        return CurrentIncrementalIdx >= IncrementalBackups.size();
    }

    bool IsCurrentIncrementalComplete() const {
        return CurrentIncrementalIdx < IncrementalBackups.size() &&
                IncrementalBackups[CurrentIncrementalIdx].Completed;
    }

    bool AreAllCurrentOperationsComplete() const {
        // If we started processing the current incremental but there are no operations at all,
        // it means no table backups were found in this incremental backup, so consider it complete
        // TODO: probably have to ensure that empty backups are impossible
        if (CurrentIncrementalStarted && InProgressOperations.empty() && CompletedOperations.empty()) {
            return true;
        }
        // Normal case: all operations have moved from InProgress to Completed
        return InProgressOperations.empty() && !CompletedOperations.empty();
    }

    void MarkCurrentIncrementalComplete() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            IncrementalBackups[CurrentIncrementalIdx].Completed = true;
        }
    }

    void MoveToNextIncremental() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            CurrentIncrementalIdx++;
            CurrentIncrementalStarted = false;

            // Reset operation tracking for next incremental
            InProgressOperations.clear();
            CompletedOperations.clear();
            TableOperations.clear();
            // Note: We don't clear InvolvedShards as it accumulates across all incrementals
        }
    }

    const TIncrementalBackup* GetCurrentIncremental() const {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            return &IncrementalBackups[CurrentIncrementalIdx];
        }

        return nullptr;
    }

    void AddIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp) {
        IncrementalBackups.emplace_back(pathId, path, timestamp);

        // Sort by timestamp to ensure chronological order
        std::sort(IncrementalBackups.begin(), IncrementalBackups.end(),
                    [](const TIncrementalBackup& a, const TIncrementalBackup& b) {
                        return a.Timestamp < b.Timestamp;
                    });
    }

    void AddCurrentIncrementalOperation(const TOperationId& opId) {
        InProgressOperations.insert(opId);
    }

    void MarkOperationComplete(const TOperationId& opId) {
        InProgressOperations.erase(opId);
        CompletedOperations.insert(opId);
    }

    bool AllCurrentIncrementalOperationsComplete() const {
        return InProgressOperations.empty() && !CompletedOperations.empty();
    }
};

struct TIncrementalBackupInfo : public TSimpleRefCount<TIncrementalBackupInfo> {
    using TPtr = TIntrusivePtr<TIncrementalBackupInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Transferring = 1,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    struct TItem {
        enum class EState: ui8 {
            Invalid = 0,
            Transferring = 1,
            Dropping = 230,
            Done = 240,
            Cancellation = 250,
            Cancelled = 251,
        };

        TPathId PathId;
        EState State;

        bool IsDone() const {
            return State == EState::Done;
        }
    };

    ui64 Id;
    EState State;
    TPathId DomainPathId;

    THashMap<TPathId, TItem> Items;

    TMaybe<TString> UserSID;
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    explicit TIncrementalBackupInfo(
            const ui64 id,
            const TPathId domainPathId)
        : Id(id)
        , DomainPathId(domainPathId)
    {}

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool IsAllItemsDone() const {
        for (const auto& item : Items) {
            if (!item.second.IsDone()) {
                return false;
            }
        }
        return true;
    }
};

struct TSecretInfo : TSimpleRefCount<TSecretInfo> {
    using TPtr = TIntrusivePtr<TSecretInfo>;

    TSecretInfo(const ui64 alterVersion)
        : AlterVersion(alterVersion)
    {
    }

    TSecretInfo(const ui64 alterVersion, NKikimrSchemeOp::TSecretDescription&& desc)
        : AlterVersion(alterVersion)
        , Description(std::move(desc))
    {
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);

        TPtr result = new TSecretInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;

        return result;
    }

    static TPtr New() {
        return new TSecretInfo(0);
    }

    static TPtr Create(NKikimrSchemeOp::TSecretDescription&& desc) {
        TPtr result = New();
        TPtr alterData = result->CreateNextVersion();
        alterData->Description = std::move(desc);

        return result;
    }

    ui64 AlterVersion = 0;
    TSecretInfo::TPtr AlterData = nullptr;
    NKikimrSchemeOp::TSecretDescription Description;
};

struct TStreamingQueryInfo : TSimpleRefCount<TStreamingQueryInfo> {
    using TPtr = TIntrusivePtr<TStreamingQueryInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
};

bool ValidateTtlSettings(const NKikimrSchemeOp::TTTLSettings& ttl,
    const TMap<ui32, TTableInfo::TColumn>& sourceColumns,
    const TMap<ui32, TTableInfo::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    const TSubDomainInfo& subDomain, TString& errStr);

TConclusion<TDuration> GetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& settings, const bool allowNonDeleteTiers);

std::optional<std::pair<i64, i64>> ValidateSequenceType(const TString& sequenceName, const TString& dataType,
    const NKikimr::NScheme::TTypeRegistry& typeRegistry, bool pgTypesEnabled, TString& errStr);

NProtoBuf::Timestamp SecondsToProtoTimeStamp(ui64 sec);

inline bool IsValidColumnName(const TString& name, bool allowSystemColumnNames = false) {
    if (!allowSystemColumnNames && name.StartsWith(SYSTEM_COLUMN_PREFIX)) {
        return false;
    }

    for (auto c: name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }

    return true;
}

// namespace NForcedCompaction {
struct TForcedCompactionInfo : TSimpleRefCount<TForcedCompactionInfo> {
    using TPtr = TIntrusivePtr<TForcedCompactionInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        InProgress = 1,
        Done = 2,
        Cancelled = 3,
        Cancelling = 4,
    };

    ui64 Id;  // TxId from the original TEvCreateRequest
    EState State = EState::Invalid; 
    TPathId TablePathId;
    TPathId SubdomainPathId;
    bool Cascade;
    ui32 MaxShardsInFlight;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    TMaybe<TString> UserSID;

    ui32 TotalShardCount = 0;
    ui32 DoneShardCount = 0; // updates only when persisting

    THashSet<TShardIdx> ShardsInFlight;

    TSet<TActorId> Subscribers;

    bool IsFinished() const;
    void AddNotifySubscriber(const TActorId& actorId);
    float CalcProgress() const;
};
// } // NForcedCompaction

}

}

Y_DECLARE_OUT_SPEC(inline, NKikimrIndexBuilder::TMeteringStats, stream, value) {
    stream << value.ShortDebugString();
}
