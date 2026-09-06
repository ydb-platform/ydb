#pragma once

#include "schemeshard_info_types_base.h"
#include "schemeshard_info_types_table_column.h"

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_table_column.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <google/protobuf/util/message_differencer.h>

namespace NKikimr {
namespace NSchemeShard {
using namespace NTableIndex;

struct TTableInfo;
struct TTableBackupRestoreResult;
struct TTableAlterInfo;

struct TTableBackupRestoreResult {
    using EKind = ETableBackupRestoreKind;

    ui64 StartDateTime; // seconds
    ui64 CompletionDateTime; // seconds
    ui32 TotalShardCount;
    ui32 SuccessShardCount;
    THashMap<TShardIdx, TTxState::TShardStatus> ShardStatuses;
    ui64 DataTotalSize;
};

struct TTableAlterInfo : TSimpleRefCount<TTableAlterInfo> {
        using TPtr = TIntrusivePtr<TTableAlterInfo>;

        ui32 NextColumnId = 1;
        ui64 AlterVersion = 0;
        TMap<ui32, TTableColumn> Columns;
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

struct TTableInfo : public TSimpleRefCount<TTableInfo> {
    using TPtr = TIntrusivePtr<TTableInfo>;
    using TCPtr = TIntrusiveConstPtr<TTableInfo>;
    using TColumn = TTableColumn;
    using TBackupRestoreResult = TTableBackupRestoreResult;
    using TAlterTableInfo = TTableAlterInfo;

    using TAlterDataPtr = TAlterTableInfo::TPtr;

    ui32 NextColumnId = 1;          // Next unallocated column id
    ui64 AlterVersion = 0;
    ui64 PartitioningVersion = 0;
    TMap<ui32, TColumn> Columns;
    TVector<ui32> KeyColumnIds;
    bool IsBackup = false;
    // True when partition rows are stored in TablePartitionsByShardIdx (keyed by ShardIdx).
    // False (default) when stored in TablePartitions/MigratedTablePartitions (keyed by position);
    // both legacy tables are scheduled for removal once migration is complete, after which this
    // flag and its branches can be deleted.
    // Toggled during the first split/merge after EnableTablePartitionsFormatShardIdx changes.
    bool PartitionsInShardIdxFormat = false;
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

    mutable ui64 LastVerifyConsistencyTime = 0;

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

    bool HasMultiColumnStatistics() const { return TableDescription.MultiColumnStatisticsSize() > 0; }
    const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TMultiColumnStatisticsDescription>& MultiColumnStatistics() const {
        return TableDescription.GetMultiColumnStatistics();
    }
    ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TMultiColumnStatisticsDescription>* MutableMultiColumnStatistics() {
        return TableDescription.MutableMultiColumnStatistics();
    }

    /**
     * Determine if the detailed metrics settings are configured for the given table.
     *
     * @return True, if the detailed metrics settings are configured for the given table
     */
    bool HasDetailedMetricsSettings() const {
        return TableDescription.HasDetailedMetricsSettings()
            && (TableDescription.GetDetailedMetricsSettings().GetStatusCase()
                    == NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured)
            && (TableDescription.GetDetailedMetricsSettings().HasConfigured());
    }

    /**
     * Return the detailed metrics settings for the given table.
     *
     * @warning This function should be called only if HasDetailedMetricsSettings() returns true.
     *
     * @return The detailed metrics settings for the given table
     */
    const NKikimrSchemeOp::TTableDetailedMetricsSettings::TConfigured& GetDetailedMetricsSettings() const {
        return TableDescription.GetDetailedMetricsSettings().GetConfigured();
    }

    /**
     * Return the modifiable version of the detailed metrics settings for the given table.
     *
     * @return The modifiable version of the detailed metrics settings for the given table
     */
    NKikimrSchemeOp::TTableDetailedMetricsSettings::TConfigured& MutableDetailedMetricsSettings() {
        return *TableDescription.MutableDetailedMetricsSettings()->MutableConfigured();
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

    static constexpr ui32 InvalidColumnId = Max<ui32>();
    // TODO(flown4qqqq):: rework this into a fast way.
    ui32 GetColumnIdByNameSlow(const TString& columnName) const;

private:
    using TPartitionsVec = TVector<TTableShardInfo*>;
    void CalculateColumnIdByName() const;

    // Stable-address store: THashMap uses separate chaining, so element references
    // survive insert.  Also serves as the O(1) ShardIdx lookup index.
    THashMap<TShardIdx, TTableShardInfo> PartitionStore;
    TPartitionsVec Partitions;  // ordered by EndOfRange; raw ptrs into PartitionStore
    TCondEraseSchedule CondEraseSchedule;
    THashMap<TShardIdx, TActorId> InFlightCondErase; // shard to pipe client
    mutable TMaybe<ui32> TTLColumnId;
    THashSet<TOperationId> SplitOpsInFlight;
    THashMap<TOperationId, TVector<TShardIdx>> ShardsInSplitMergeByOpId;
    THashMap<TShardIdx, TOperationId> ShardsInSplitMergeByShards;
    ui64 ExpectedPartitionCount = 0; // number of partitions after all in-flight splits/merges are finished
    TAggregatedStats Stats;
    bool ShardsStatsDetached = false;

    TTableShardInfo* FindPartition(const TShardIdx& shardIdx) {
        return PartitionStore.FindPtr(shardIdx);
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

    static TIntrusivePtr<TTableInfo> DeepCopy(const TTableInfo& other) {
        TIntrusivePtr<TTableInfo> copy(new TTableInfo(other));
        // Partitions holds raw pointers into PartitionStore; after the value copy
        // they point into other's store — rebuild them to point into the copy's.
        copy->Partitions.resize(other.Partitions.size());
        for (ui64 i = 0; i < other.Partitions.size(); ++i) {
            copy->Partitions[i] = copy->PartitionStore.FindPtr(other.Partitions[i]->ShardIdx);
            Y_ABORT_UNLESS(copy->Partitions[i]);
        }

        copy->VerifyConsistency();

        return copy;
    }

    struct TCreateAlterDataFeatureFlags {
        bool EnableTablePgTypes;
        bool EnableTableDatetime64;
        bool EnableParameterizedDecimal;
        bool EnableDetailedMetrics;
        bool EnableColumnStatistics = false;
        bool EnableGeneratedStored = false;
        bool EnableGeneratedVirtual = false;
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
    // Rebuild PartitionStore/Partitions from newPartitioning; Stats are already correct
    // (caller is a DeepCopy of a table with the same physical shard set).
    void MovePartitioning(TVector<TTableShardInfo>&& newPartitioning);
    // Rebuild PartitionStore/Partitions and Stats from scratch for all-new shard IDs
    // (caller is a fresh dst table whose old placeholder shards had zero stats).
    void CopyPartitioning(TVector<TTableShardInfo>&& newPartitioning);

    // O(N) consistency check across Partitions, PartitionStore, Stats, and CondEraseSchedule.
    void VerifyConsistency() const;

    // In-place split/merge: replaces the contiguous src shard range with dst shards.
    void ApplySplitMerge(TVector<TTableShardInfo>&& dstPartitions, const TVector<TShardIdx>& removedShards, ui64 splitFirstIdx, TInstant now);

    const TVector<TTableShardInfo*>& GetPartitions() const {
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

    const THashMap<TShardIdx, TTableShardInfo>& GetPartitionStore() const {
        return PartitionStore;
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
        if (CondEraseSchedule.Empty()) {
            return nullptr;
        }
        const TShardIdx& shardIdx = CondEraseSchedule.Top().second;
        const auto* p = PartitionStore.FindPtr(shardIdx);
        Y_ABORT_UNLESS(p);
        return p;
    }

    // Schedule any partition not already in the schedule or in-flight.
    void ScheduleAllCondErase() {
        for (const auto* p : Partitions) {
            if (!CondEraseSchedule.Contains(p->ShardIdx) && !InFlightCondErase.contains(p->ShardIdx)) {
                CondEraseSchedule.Push(p->NextCondErase, p->ShardIdx);
            }
        }
    }

    void ClearCondEraseSchedule() {
        CondEraseSchedule.Clear();
        InFlightCondErase.clear();
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
        CondEraseSchedule.Pop();
    }

    void RescheduleCondErase(const TShardIdx& shardIdx) {
        Y_ENSURE(InFlightCondErase.contains(shardIdx));

        auto* p = FindPartition(shardIdx);
        Y_ENSURE(p);

        CondEraseSchedule.Push(p->NextCondErase, shardIdx);
        InFlightCondErase.erase(shardIdx);
    }

    void UpdateNextCondErase(const TShardIdx& shardIdx, const TInstant& now, const TDuration& next) {
        auto* p = FindPartition(shardIdx);
        Y_ENSURE(p);

        p->LastCondErase = now;
        p->NextCondErase = now + next;
        p->LastCondEraseLag = TDuration::Zero();
    }

    bool IsUsingSequence(const TString& name) {
        for (const auto& pr : Columns) {
            if (pr.second.DefaultKind == ETableColumnDefaultKind::FromSequence &&
                pr.second.DefaultValue == name)
            {
                // A column scheduled to be dropped by the pending alter no longer keeps the
                // sequence alive. This lets a single ALTER drop a serial column and cascade
                // a DropSequence sub-operation for its backing sequence in the same operation:
                // the AlterTable part is proposed first (marking the column for deletion in
                // AlterData), so the DropSequence part that follows does not see the sequence
                // as still in use.
                if (AlterData) {
                    auto it = AlterData->Columns.find(pr.first);
                    if (it != AlterData->Columns.end() && it->second.DeleteVersion == AlterData->AlterVersion) {
                        continue;
                    }
                }
                return true;
            }
        }
        return false;
    }
};

bool ValidateTtlSettings(const NKikimrSchemeOp::TTTLSettings& ttl,
    const TMap<ui32, TTableColumn>& sourceColumns,
    const TMap<ui32, TTableColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    const TSubDomainInfo& subDomain, TString& errStr);

bool ValidateTableDetailedMetricsSettings(
    bool forCreate,
    const NKikimrSchemeOp::TTableDetailedMetricsSettings& metricsSettings,
    TString& errorString
);

TConclusion<TDuration> GetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& settings, const bool allowNonDeleteTiers);

inline bool IsValidColumnName(const TString& name, bool allowSystemColumnNames = false) {
    // The fulltext rowid column carries the system prefix but is user-facing: callers may
    // pre-create it (or ALTER ADD it) and the schemeshard auto-provisions it. Always accept it,
    // so a plain user CREATE/ALTER naming it is not rejected as a forbidden system column.
    if (!allowSystemColumnNames
        && name != NTableIndex::NFulltext::RowIdColumn
        && name.StartsWith(SYSTEM_COLUMN_PREFIX))
    {
        return false;
    }

    for (auto c: name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }

    return true;
}

} // namespace NSchemeShard
} // namespace NKikimr
