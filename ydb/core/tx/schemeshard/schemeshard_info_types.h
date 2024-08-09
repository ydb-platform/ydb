#pragma once

#include "schemeshard.h"
#include "schemeshard_types.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_path_element.h"
#include "schemeshard_identificators.h"
#include "schemeshard_schema.h"
#include "olap/schema/schema.h"
#include "olap/schema/update.h"

#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/control/immediate_control_board_impl.h>

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_table_column.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/core/base/tx_processing.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/util/counted_leaky_bucket.h>
#include <ydb/core/util/pb.h>

#include <ydb/library/login/protos/login.pb.h>

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/generic/guid.h>

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
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
        icb->RegisterSharedControl(SplitMergePartCountLimit,        "SchemeShard_SplitMergePartCountLimit");
        icb->RegisterSharedControl(FastSplitSizeThreshold,          "SchemeShard_FastSplitSizeThreshold");
        icb->RegisterSharedControl(FastSplitRowCountThreshold,      "SchemeShard_FastSplitRowCountThreshold");
        icb->RegisterSharedControl(FastSplitCpuPercentageThreshold, "SchemeShard_FastSplitCpuPercentageThreshold");

        icb->RegisterSharedControl(SplitByLoadEnabled,              "SchemeShard_SplitByLoadEnabled");
        icb->RegisterSharedControl(SplitByLoadMaxShardsDefault,     "SchemeShard_SplitByLoadMaxShardsDefault");
        icb->RegisterSharedControl(MergeByLoadMinUptimeSec,         "SchemeShard_MergeByLoadMinUptimeSec");
        icb->RegisterSharedControl(MergeByLoadMinLowLoadDurationSec,"SchemeShard_MergeByLoadMinLowLoadDurationSec");

        icb->RegisterSharedControl(ForceShardSplitDataSize,         "SchemeShardControls.ForceShardSplitDataSize");
        icb->RegisterSharedControl(DisableForceShardSplit,          "SchemeShardControls.DisableForceShardSplit");
    }

    TForceShardSplitSettings GetForceShardSplitSettings() const {
        return TForceShardSplitSettings{
            .ForceShardSplitDataSize = ui64(ForceShardSplitDataSize),
            .DisableForceShardSplit = ui64(DisableForceShardSplit) != 0,
        };
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

    static NKikimrSchemeOp::TPartitionConfig DefaultConfig(const TAppData* appData);
    static bool ApplyChanges(
        NKikimrSchemeOp::TPartitionConfig& result,
        const NKikimrSchemeOp::TPartitionConfig& src, const NKikimrSchemeOp::TPartitionConfig& changes,
        const TAppData* appData, TString& errDescr);

    static bool ApplyChangesInColumnFamilies(
        NKikimrSchemeOp::TPartitionConfig& result,
        const NKikimrSchemeOp::TPartitionConfig& src, const NKikimrSchemeOp::TPartitionConfig& changes,
        TString& errDescr);

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
        const NKikimrSchemeOp::TCompactionPolicy& policy,
        TString& err);

    static bool VerifyCommandOnFrozenTable(
        const NKikimrSchemeOp::TPartitionConfig& srcConfig,
        const NKikimrSchemeOp::TPartitionConfig& dstConfig);

};

struct TPartitionStats {
    // Latest timestamps when CPU usage exceeded 2%, 5%, 10%, 20%, 30%
    struct TTopUsage {
        TInstant Last2PercentLoad;
        TInstant Last5PercentLoad;
        TInstant Last10PercentLoad;
        TInstant Last20PercentLoad;
        TInstant Last30PercentLoad;

        const TTopUsage& Update(const TTopUsage& usage) {
            Last2PercentLoad  = std::max(Last2PercentLoad,  usage.Last2PercentLoad);
            Last5PercentLoad  = std::max(Last5PercentLoad,  usage.Last5PercentLoad);
            Last10PercentLoad = std::max(Last10PercentLoad, usage.Last10PercentLoad);
            Last20PercentLoad = std::max(Last20PercentLoad, usage.Last20PercentLoad);
            Last30PercentLoad = std::max(Last30PercentLoad, usage.Last30PercentLoad);
            return *this;
        }
    };

    TMessageSeqNo SeqNo;

    ui64 RowCount = 0;
    ui64 DataSize = 0;
    ui64 IndexSize = 0;

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

    // True when PartOwners has parts from other tablets
    bool HasBorrowedData = false;

    // True when lent parts to other tablets
    bool HasLoanedData = false;

    // Tablet actor started at
    TInstant StartTime;

    TTopUsage TopUsage;

    void SetCurrentRawCpuUsage(ui64 rawCpuUsage, TInstant now) {
        CPU = rawCpuUsage;
        float percent = rawCpuUsage * 0.000001 * 100;
        if (percent >= 2)
            TopUsage.Last2PercentLoad = now;
        if (percent >= 5)
            TopUsage.Last5PercentLoad = now;
        if (percent >= 10)
            TopUsage.Last10PercentLoad = now;
        if (percent >= 20)
            TopUsage.Last20PercentLoad = now;
        if (percent >= 30)
            TopUsage.Last30PercentLoad = now;
    }

    ui64 GetCurrentRawCpuUsage() const {
        return CPU;
    }

    float GetLatestMaxCpuUsagePercent(TInstant since) const {
        // TODO: fix the case when stats were not collected yet

        if (TopUsage.Last30PercentLoad > since)
            return 40;
        if (TopUsage.Last20PercentLoad > since)
            return 30;
        if (TopUsage.Last10PercentLoad > since)
            return 20;
        if (TopUsage.Last5PercentLoad > since)
            return 10;
        if (TopUsage.Last2PercentLoad > since)
            return 5;

        return 2;
    }

private:
    ui64 CPU = 0;
};

struct TAggregatedStats {
    TPartitionStats Aggregated;
    THashMap<TShardIdx, TPartitionStats> PartitionStats;
    THashMap<TPathId, TPartitionStats> TableStats;
    size_t PartitionStatsUpdated = 0;

    void UpdateShardStats(TShardIdx datashardIdx, const TPartitionStats& newStats);
    void UpdateTableStats(const TPathId& pathId, const TPartitionStats& newStats);
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
        THashMap<ui32, TColumn> Columns;
        TVector<ui32> KeyColumnIds;
        bool IsBackup = false;

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
    THashMap<ui32, TColumn> Columns;
    TVector<ui32> KeyColumnIds;
    bool IsBackup = false;
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

    const NKikimrSchemeOp::TPartitionConfig& PartitionConfig() const { return TableDescription.GetPartitionConfig(); }
    NKikimrSchemeOp::TPartitionConfig& MutablePartitionConfig() { return *TableDescription.MutablePartitionConfig(); }

    bool HasReplicationConfig() { return TableDescription.HasReplicationConfig(); }
    const NKikimrSchemeOp::TTableReplicationConfig& ReplicationConfig() { return TableDescription.GetReplicationConfig(); }
    NKikimrSchemeOp::TTableReplicationConfig& MutableReplicationConfig() { return *TableDescription.MutableReplicationConfig(); }

    bool IsAsyncReplica() const {
        switch (TableDescription.GetReplicationConfig().GetMode()) {
            case NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE:
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
    {
        TableDescription.Swap(alterData.TableDescriptionFull.Get());
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

    static TAlterDataPtr CreateAlterData(
        TPtr source,
        NKikimrSchemeOp::TTableDescription& descr,
        const NScheme::TTypeRegistry& typeRegistry,
        const TSchemeLimits& limits, const TSubDomainInfo& subDomain,
        bool pgTypesEnabled,
        bool datetime64TypesEnabled,
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
        Y_ABORT_UNLESS(room.GetId() == 0);
        auto rooms = MutablePartitionConfig().MutableStorageRooms();
        rooms->Clear();
        rooms->Add()->CopyFrom(room);
    }


    void InitAlterData() {
        if (!AlterData) {
            AlterData = new TTableInfo::TAlterTableInfo;
            AlterData->AlterVersion = AlterVersion + 1; // calc next AlterVersion
            AlterData->NextColumnId = NextColumnId;
        }
    }

    void PrepareAlter(TAlterDataPtr alterData) {
        Y_ABORT_UNLESS(alterData, "No alter data at Alter prepare");
        Y_ABORT_UNLESS(alterData->AlterVersion == AlterVersion + 1);
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

    void UpdateShardStats(TShardIdx datashardIdx, const TPartitionStats& newStats);

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
                            const TTableInfo* mainTableForIndex) const;

    bool CheckCanMergePartitions(const TSplitSettings& splitSettings,
                                 const TForceShardSplitSettings& forceShardSplitSettings,
                                 TShardIdx shardIdx, TVector<TShardIdx>& shardsToMerge,
                                 const TTableInfo* mainTableForIndex) const;

    bool CheckSplitByLoad(
            const TSplitSettings& splitSettings, TShardIdx shardIdx,
            ui64 dataSize, ui64 rowCount,
            const TTableInfo* mainTableForIndex) const;

    bool IsSplitBySizeEnabled(const TForceShardSplitSettings& params) const {
        // Respect unspecified SizeToSplit when force shard splits are disabled
        if (params.DisableForceShardSplit && PartitionConfig().GetPartitioningPolicy().GetSizeToSplit() == 0) {
            return false;
        }
        // Auto split is always enabled, unless table is using external blobs
        return !PartitionConfigHasExternalBlobsEnabled(PartitionConfig());
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
        if (PartitionConfigHasExternalBlobsEnabled(PartitionConfig())) {
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

    bool ShouldSplitBySize(ui64 dataSize, const TForceShardSplitSettings& params) const {
        if (!IsSplitBySizeEnabled(params)) {
            return false;
        }
        // When shard is over the maximum size we split even when over max partitions
        if (dataSize >= params.ForceShardSplitDataSize && !params.DisableForceShardSplit) {
            return true;
        }
        // Otherwise we split when we may add one more partition
        return Partitions.size() < GetMaxPartitionsCount() && dataSize >= GetShardSizeToSplit(params);
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
        Y_ABORT_UNLESS(shardInfo && shardIdx == shardInfo->ShardIdx);

        InFlightCondErase[shardIdx] = TActorId();
        CondEraseSchedule.pop();
    }

    void RescheduleCondErase(const TShardIdx& shardIdx) {
        Y_ABORT_UNLESS(InFlightCondErase.contains(shardIdx));

        auto it = FindPartition(shardIdx);
        Y_ABORT_UNLESS(it != Partitions.end());

        CondEraseSchedule.push(it);
        InFlightCondErase.erase(shardIdx);
    }

    void ScheduleNextCondErase(const TShardIdx& shardIdx, const TInstant& now, const TDuration& next) {
        Y_ABORT_UNLESS(InFlightCondErase.contains(shardIdx));

        auto it = FindPartition(shardIdx);
        Y_ABORT_UNLESS(it != Partitions.end());

        it->LastCondErase = now;
        it->NextCondErase = now + next;
        it->LastCondEraseLag = TDuration::Zero();

        CondEraseSchedule.push(it);
        InFlightCondErase.erase(shardIdx);
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
        THashSet<ui32> ParentPartitionIds;
        THashSet<ui32> ChildPartitionIds;

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

    TString PreSerializedPathDescription; // Cached path description
    TString PreSerializedPartitionsDescription; // Cached partition description

    TTopicStats Stats;

    void AddPartition(TShardIdx shardIdx, TTopicTabletInfo::TTopicPartitionInfo* partition) {
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
            Y_ABORT_UNLESS(it != Partitions.end(),
                     "Partition %" PRIu32 " has parent partition %" PRIu32 " which doesn't exists", partition.GroupId,
                     parent);
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

        Y_ABORT_UNLESS(TotalPartitionCount);
        Y_ABORT_UNLESS(MaxPartsPerTablet);

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
            Y_ABORT_UNLESS(parseOk, "Previously serialized pq tablet config cannot be parsed");
        }
        return tabletConfig;
    }

    void PrepareAlter(TTopicInfo::TPtr alterData) {
        Y_ABORT_UNLESS(alterData, "No alter data at Alter prepare");
        alterData->AlterVersion = AlterVersion + 1;
        Y_ABORT_UNLESS(alterData->TotalGroupCount);
        Y_ABORT_UNLESS(alterData->TotalPartitionCount);
        Y_ABORT_UNLESS(0 < alterData->ActivePartitionCount && alterData->ActivePartitionCount <= alterData->TotalPartitionCount);
        Y_ABORT_UNLESS(alterData->NextPartitionId);
        Y_ABORT_UNLESS(alterData->MaxPartsPerTablet);
        alterData->KeySchema = KeySchema;
        alterData->BalancerTabletID = BalancerTabletID;
        alterData->BalancerShardIdx = BalancerShardIdx;
        AlterData = alterData;
    }

    void FinishAlter() {
        Y_ABORT_UNLESS(AlterData, "No alter data at Alter complete");
        TotalGroupCount = AlterData->TotalGroupCount;
        NextPartitionId = AlterData->NextPartitionId;
        TotalPartitionCount = AlterData->TotalPartitionCount;
        ActivePartitionCount = AlterData->ActivePartitionCount;
        MaxPartsPerTablet = AlterData->MaxPartsPerTablet;
        if (!AlterData->TabletConfig.empty())
            TabletConfig = std::move(AlterData->TabletConfig);
        ++AlterVersion;
        Y_ABORT_UNLESS(BalancerTabletID == AlterData->BalancerTabletID || !HasBalancer());
        Y_ABORT_UNLESS(AlterData->HasBalancer());
        Y_ABORT_UNLESS(AlterData->BalancerShardIdx);
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
        Y_ABORT_UNLESS(Version < version);
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
            Y_ABORT_UNLESS(other.GetPlanResolution() == 0 || other.GetPlanResolution() == planResolution);
            ProcessingParams.SetPlanResolution(planResolution);
        }

        if (timeCastBucketsMediator) {
            Y_ABORT_UNLESS(other.GetTCB() == 0 || other.GetTCB() == timeCastBucketsMediator);
            ProcessingParams.SetTimeCastBucketsPerMediator(timeCastBucketsMediator);
        }

        for (auto& toAdd: additionalPools) {
            StoragePools.push_back(toAdd);
        }
    }

    void SetSchemeLimits(const TSchemeLimits& limits) {
        SchemeLimits = limits;
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
        Y_ABORT_UNLESS(ProcessingParams.GetVersion() < version);
        ProcessingParams.SetVersion(version);
    }

    void SetAlter(TPtr alterData) {
        Y_ABORT_UNLESS(alterData);
        Y_ABORT_UNLESS(GetVersion() < alterData->GetVersion());
        AlterData = alterData;
    }

    void SetStoragePools(TStoragePools& storagePools, ui64 subDomainVersion) {
        Y_ABORT_UNLESS(GetVersion() < subDomainVersion);
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

    void IncPathsInside(ui64 delta = 1, bool isBackup = false) {
        Y_ABORT_UNLESS(Max<ui64>() - PathsInsideCount >= delta);
        PathsInsideCount += delta;

        if (isBackup) {
            Y_ABORT_UNLESS(Max<ui64>() - BackupPathsCount >= delta);
            BackupPathsCount += delta;
        }
    }

    void DecPathsInside(ui64 delta = 1, bool isBackup = false) {
        Y_VERIFY_S(PathsInsideCount >= delta, "PathsInsideCount: " << PathsInsideCount << " delta: " << delta);
        PathsInsideCount -= delta;

        if (isBackup) {
            Y_VERIFY_S(BackupPathsCount >= delta, "BackupPathsCount: " << BackupPathsCount << " delta: " << delta);
            BackupPathsCount -= delta;
        }
    }

    ui64 GetPQPartitionsInside() const {
        return PQPartitionsInsideCount;
    }

    void SetPQPartitionsInside(ui64 val) {
        PQPartitionsInsideCount = val;
    }

    void IncPQPartitionsInside(ui64 delta = 1) {
        Y_ABORT_UNLESS(Max<ui64>() - PQPartitionsInsideCount >= delta);
        PQPartitionsInsideCount += delta;
    }

    void DecPQPartitionsInside(ui64 delta = 1) {
        Y_VERIFY_S(PQPartitionsInsideCount >= delta, "PQPartitionsInsideCount: " << PQPartitionsInsideCount << " delta: " << delta);
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
        Y_ABORT_UNLESS(Max<ui64>() - PQReservedStorage >= delta);
        PQReservedStorage += delta;
    }

    void DecPQReservedStorage(ui64 delta = 1) {
        Y_VERIFY_S(PQReservedStorage >= delta, "PQReservedStorage: " << PQReservedStorage << " delta: " << delta);
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

    void ActualizeAlterData(const THashMap<TShardIdx, TShardInfo>& allShards, TInstant now, bool isExternal, IQuotaCounters* counters) {
        Y_ABORT_UNLESS(AlterData);

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

    void AddInternalShard(TShardIdx shardId, bool isBackup = false) {
        InternalShards.insert(shardId);
        if (isBackup) {
            BackupShards.insert(shardId);
        }
    }

    const THashSet<TShardIdx>& GetInternalShards() const {
        return InternalShards;
    }

    void AddInternalShards(const TTxState& txState, bool isBackup = false) {
        for (auto txShard: txState.Shards) {
            if (txShard.Operation != TTxState::CreateParts) {
                continue;
            }
            AddInternalShard(txShard.Idx, isBackup);
        }
    }

    void RemoveInternalShard(TShardIdx shardIdx) {
        auto it = InternalShards.find(shardIdx);
        Y_VERIFY_S(it != InternalShards.end(), "shardIdx: " << shardIdx);
        InternalShards.erase(it);
        BackupShards.erase(shardIdx);
    }

    const THashSet<TShardIdx>& GetSequenceShards() const {
        return SequenceShards;
    }

    void AddSequenceShard(const TShardIdx& shardIdx) {
        SequenceShards.insert(shardIdx);
    }

    void RemoveSequenceShard(const TShardIdx& shardIdx) {
        auto it = SequenceShards.find(shardIdx);
        Y_VERIFY_S(it != SequenceShards.end(), "shardIdx: " << shardIdx);
        SequenceShards.erase(it);
    }

    const NKikimrSubDomains::TProcessingParams& GetProcessingParams() const {
        return ProcessingParams;
    }

    TTabletId GetCoordinator(TTxId txId) const {
        Y_ABORT_UNLESS(IsSupportTransactions());
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
        Y_VERIFY_S(schemeshards.size() <= 1, "size was: " << schemeshards.size());
        if (schemeshards.size()) {
            ProcessingParams.SetSchemeShard(ui64(schemeshards.front()));
        }

        ProcessingParams.ClearHive();
        TVector<TTabletId> hives = FilterPrivateTablets(ETabletType::Hive, allShards);
        Y_VERIFY_S(hives.size() <= 1, "size was: " << hives.size());
        if (hives.size()) {
            ProcessingParams.SetHive(ui64(hives.front()));
            SetSharedHive(InvalidTabletId); // set off shared hive when our own hive has found
        }

        ProcessingParams.ClearSysViewProcessor();
        TVector<TTabletId> sysViewProcessors = FilterPrivateTablets(ETabletType::SysViewProcessor, allShards);
        Y_VERIFY_S(sysViewProcessors.size() <= 1, "size was: " << sysViewProcessors.size());
        if (sysViewProcessors.size()) {
            ProcessingParams.SetSysViewProcessor(ui64(sysViewProcessors.front()));
        }

        ProcessingParams.ClearStatisticsAggregator();
        TVector<TTabletId> statisticsAggregators = FilterPrivateTablets(ETabletType::StatisticsAggregator, allShards);
        Y_VERIFY_S(statisticsAggregators.size() <= 1, "size was: " << statisticsAggregators.size());
        if (statisticsAggregators.size()) {
            ProcessingParams.SetStatisticsAggregator(ui64(statisticsAggregators.front()));
        }

        ProcessingParams.ClearGraphShard();
        TVector<TTabletId> graphs = FilterPrivateTablets(ETabletType::GraphShard, allShards);
        Y_VERIFY_S(graphs.size() <= 1, "size was: " << graphs.size());
        if (graphs.size()) {
            ProcessingParams.SetGraphShard(ui64(graphs.front()));
        }
    }

    void InitializeAsGlobal(NKikimrSubDomains::TProcessingParams&& processingParams) {
        InitiatedAsGlobal = true;

        Y_ABORT_UNLESS(processingParams.GetPlanResolution());
        Y_ABORT_UNLESS(processingParams.GetTimeCastBucketsPerMediator());

        ui64 version = ProcessingParams.GetVersion();
        ProcessingParams = std::move(processingParams);
        ProcessingParams.SetVersion(version);

        CoordinatorSelector = new TCoordinators(ProcessingParams);
    }

    void AggrDiskSpaceUsage(IQuotaCounters* counters, const TPartitionStats& newAggr, const TPartitionStats& oldAggr = {});

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
            Y_ABORT_UNLESS(DeclaredSchemeQuotas->SerializeToString(&prev));
            Y_ABORT_UNLESS(declaredSchemeQuotas.SerializeToString(&next));
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

    bool HasSecurityState() const {
        return SecurityState.PublicKeysSize() > 0;
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
        Y_ABORT_UNLESS(serverlessComputeResourcesMode, "Can't set ServerlessComputeResourcesMode to unspecified");
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
        Y_ABORT_UNLESS(alterData, "No alter data at Alter preparation");
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
        Y_ABORT_UNLESS(AlterData, "No alter data at Alter rollback");
        AlterData.Reset();
    }

    void FinishAlter() {
        Y_ABORT_UNLESS(AlterData, "No alter data at Alter completion");
        DefaultPartitionCount = AlterData->DefaultPartitionCount;
        ExplicitChannelProfileCount = AlterData->ExplicitChannelProfileCount;
        VolumeConfig.CopyFrom(AlterData->VolumeConfig);
        ++AlterVersion;
        Y_ABORT_UNLESS(AlterVersion == AlterData->AlterVersion);
        Y_ABORT_UNLESS(VolumeTabletId == AlterData->VolumeTabletId || !HasVolumeTablet());
        Y_ABORT_UNLESS(AlterData->HasVolumeTablet());
        Y_ABORT_UNLESS(AlterData->VolumeShardIdx);
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
            Y_VERIFY_S(itShard != allShards.end(), "No shard with shardIdx " << shardIdx);
            TTabletId tabletId = itShard->second.TabletID;

            if (partInfo.AlterVersion <= AlterVersion) {
                Y_ABORT_UNLESS(partInfo.PartitionId < DefaultPartitionCount,
                    "Wrong PartitionId %" PRIu32, partInfo.PartitionId);
                TabletCache.Tablets[partInfo.PartitionId] = tabletId;
            }
        }

        // Verify there are no missing tabletIds
        for (ui32 idx = 0; idx < TabletCache.Tablets.size(); ++idx) {
            TTabletId tabletId = TabletCache.Tablets[idx];
            Y_VERIFY_S(tabletId, "Unassigned tabletId"
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
        Y_ABORT_UNLESS(!AlterConfig);
        Y_ABORT_UNLESS(!AlterVersion);

        AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
        AlterConfig->CopyFrom(alterConfig);

        Y_ABORT_UNLESS(!AlterConfig->GetBlockSize());
        AlterConfig->SetBlockSize(Config.GetBlockSize());

        AlterVersion = Version + 1;
    }

    void ForgetAlter() {
        Y_ABORT_UNLESS(AlterConfig);
        Y_ABORT_UNLESS(AlterVersion);

        AlterConfig.Reset();
        AlterVersion = 0;
    }

    void FinishAlter() {
        Y_ABORT_UNLESS(AlterConfig);
        Y_ABORT_UNLESS(AlterVersion);

        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ABORT_UNLESS(Version == AlterVersion);

        ForgetAlter();
    }

    TFileStoreSpace GetFileStoreSpace() const {
        auto space = GetFileStoreSpace(Config);

        if (AlterConfig) {
            const auto alterSpace = GetFileStoreSpace(*AlterConfig);
            space.SSD = Max(space.SSD, alterSpace.SSD);
            space.HDD = Max(space.HDD, alterSpace.HDD);
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
                space.SSD += blockCount * blockSize;
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
        Y_ABORT_UNLESS(AlterConfig, "No alter config at Alter completion");
        Y_ABORT_UNLESS(AlterVersion, "No alter version at Alter completion");
        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ABORT_UNLESS(Version == AlterVersion);
        AlterConfig.Reset();
        AlterVersion = 0;
    }
};

struct TTableIndexInfo : public TSimpleRefCount<TTableIndexInfo> {
    using TPtr = TIntrusivePtr<TTableIndexInfo>;
    using EType = NKikimrSchemeOp::EIndexType;
    using EState = NKikimrSchemeOp::EIndexState;

    TTableIndexInfo(ui64 version, EType type, EState state)
        : AlterVersion(version)
        , Type(type)
        , State(state)
    {}

    TTableIndexInfo(const TTableIndexInfo&) = default;

    TPtr CreateNextVersion() {
        this->AlterData = this->GetNextVersion();
        return this->AlterData;
    }

    TPtr GetNextVersion() const {
        Y_ABORT_UNLESS(AlterData == nullptr);
        TPtr result = new TTableIndexInfo(*this);
        ++result->AlterVersion;
        return result;
    }

    static TPtr NotExistedYet(EType type) {
        return new TTableIndexInfo(0, type, EState::EIndexStateInvalid);
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexCreationConfig& config, TString& errMsg) {
        if (!config.KeyColumnNamesSize()) {
            errMsg += TStringBuilder() << "no key columns in index creation config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        Y_ABORT_UNLESS(alterData->IndexKeys.size());
        alterData->IndexDataColumns.assign(config.GetDataColumnNames().begin(), config.GetDataColumnNames().end());
        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        return result;
    }

    ui64 AlterVersion = 1;
    EType Type;
    EState State;

    TVector<TString> IndexKeys;
    TVector<TString> IndexDataColumns;

    TTableIndexInfo::TPtr AlterData = nullptr;
};

struct TCdcStreamInfo : public TSimpleRefCount<TCdcStreamInfo> {
    using TPtr = TIntrusivePtr<TCdcStreamInfo>;
    using EMode = NKikimrSchemeOp::ECdcStreamMode;
    using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
    using EState = NKikimrSchemeOp::ECdcStreamState;

    // shards of the table
    struct TShardStatus {
        NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus Status;

        explicit TShardStatus(NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status)
            : Status(status)
        {}
    };

    TCdcStreamInfo(ui64 version, EMode mode, EFormat format, bool vt, const TDuration& rt, const TString& awsRegion, EState state)
        : AlterVersion(version)
        , Mode(mode)
        , Format(format)
        , VirtualTimestamps(vt)
        , ResolvedTimestamps(rt)
        , AwsRegion(awsRegion)
        , State(state)
    {}

    TCdcStreamInfo(const TCdcStreamInfo&) = default;

    TPtr CreateNextVersion() {
        Y_ABORT_UNLESS(AlterData == nullptr);
        TPtr result = new TCdcStreamInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static TPtr New(EMode mode, EFormat format, bool vt, const TDuration& rt, const TString& awsRegion) {
        return new TCdcStreamInfo(0, mode, format, vt, rt, awsRegion, EState::ECdcStreamStateInvalid);
    }

    static TPtr Create(const NKikimrSchemeOp::TCdcStreamDescription& desc) {
        TPtr result = New(desc.GetMode(), desc.GetFormat(), desc.GetVirtualTimestamps(),
            TDuration::MilliSeconds(desc.GetResolvedTimestampsIntervalMs()), desc.GetAwsRegion());
        TPtr alterData = result->CreateNextVersion();
        alterData->State = EState::ECdcStreamStateReady;
        if (desc.HasState()) {
            alterData->State = desc.GetState();
        }

        return result;
    }

    ui64 AlterVersion = 1;
    EMode Mode;
    EFormat Format;
    bool VirtualTimestamps;
    TDuration ResolvedTimestamps;
    TString AwsRegion;
    EState State;

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
        Y_ABORT_UNLESS(AlterData == nullptr);
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
        Y_ABORT_UNLESS(AlterData == nullptr);

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
        Y_ABORT_UNLESS(!AlterData);
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
        Waiting,
        CreateExportDir,
        CopyTables,
        Transferring,
        Done = 240,
        Dropping = 241,
        Dropped = 242,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        YT = 0,
        S3,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        TString SourcePathName;
        TPathId SourcePathId;

        EState State = EState::Waiting;
        ESubState SubState = ESubState::AllocateTxId;
        TTxId WaitTxId = InvalidTxId;
        TString Issue;

        TItem() = default;

        explicit TItem(const TString& sourcePathName, const TPathId sourcePathId)
            : SourcePathName(sourcePathName)
            , SourcePathId(sourcePathId)
        {
        }

        TString ToString(ui32 idx) const;

        static bool IsDone(const TItem& item);
        static bool IsDropped(const TItem& item);
    };

    ui64 Id;
    TString Uid;
    EKind Kind;
    TString Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;
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

    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TString& settings,
            const TPathId domainPathId)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , Settings(settings)
        , DomainPathId(domainPathId)
    {
    }

    template <typename TSettingsPB>
    explicit TExportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TSettingsPB& settingsPb,
            const TPathId domainPathId)
        : TExportInfo(id, uid, kind, SerializeSettings(settingsPb), domainPathId)
    {
    }

    bool IsValid() const {
        return State != EState::Invalid;
    }

    bool IsPreparing() const {
        return State == EState::CreateExportDir || State == EState::CopyTables;
    }

    bool IsWorking() const {
        return State == EState::Transferring;
    }

    bool IsDropping() const {
        return State == EState::Dropping;
    }

    bool IsCancelling() const {
        return State == EState::Cancellation;
    }

    bool IsInProgress() const {
        return IsPreparing() || IsWorking() || IsDropping() || IsCancelling();
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

private:
    template <typename TSettingsPB>
    static TString SerializeSettings(const TSettingsPB& settings) {
        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD settings.SerializeToString(&serialized);
        return serialized;
    }

}; // TExportInfo
// } // NExport

// namespace NImport {
struct TImportInfo: public TSimpleRefCount<TImportInfo> {
    using TPtr = TIntrusivePtr<TImportInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Waiting,
        GetScheme,
        CreateTable,
        Transferring,
        BuildIndexes,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        S3 = 0,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        TString DstPathName;
        TPathId DstPathId;
        Ydb::Table::CreateTableRequest Scheme;

        EState State = EState::GetScheme;
        ESubState SubState = ESubState::AllocateTxId;
        TTxId WaitTxId = InvalidTxId;
        int NextIndexIdx = 0;
        TString Issue;

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

    ui64 Id;
    TString Uid;
    EKind Kind;
    Ydb::Import::ImportFromS3Settings Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;

    EState State = EState::Invalid;
    TString Issue;
    TVector<TItem> Items;

    TSet<TActorId> Subscribers;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    explicit TImportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const Ydb::Import::ImportFromS3Settings& settings,
            const TPathId domainPathId)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , Settings(settings)
        , DomainPathId(domainPathId)
    {
    }

    TString ToString() const;

    bool IsFinished() const;

    void AddNotifySubscriber(const TActorId& actorId);

}; // TImportInfo
// } // NImport

class TBillingStats {
public:
    TBillingStats() = default;
    TBillingStats(ui64 rows, ui64 bytes);
    TBillingStats(const TBillingStats& other);

    TBillingStats& operator = (const TBillingStats& other);

    TBillingStats operator - (const TBillingStats& other) const;
    TBillingStats& operator -= (const TBillingStats& other);

    TBillingStats operator + (const TBillingStats& other) const;
    TBillingStats& operator += (const TBillingStats& other);

    bool operator < (const TBillingStats& other) const;
    bool operator <= (const TBillingStats& other) const;
    bool operator == (const TBillingStats& other) const;

    operator bool () const;

    TString ToString() const;

    ui64 GetRows() const;
    ui64 GetBytes() const;

private:
    ui64 Rows = 0;
    ui64 Bytes = 0;
};

struct TIndexBuildInfo: public TSimpleRefCount<TIndexBuildInfo> {
    using TPtr = TIntrusivePtr<TIndexBuildInfo>;

    struct TLimits {
        ui32 MaxBatchRows = 100;
        ui32 MaxBatchBytes = 1 << 20;
        ui32 MaxShards = 100;
        ui32 MaxRetries = 50;
    };
    TLimits Limits;

    enum class EState: ui32 {
        Invalid = 0,
        AlterMainTable = 5,
        Locking = 10,
        GatheringStatistics = 20,
        Initiating = 30,
        Filling = 40,
        Applying = 50,
        Unlocking = 60,
        Done = 200,

        Cancellation_Applying = 350,
        Cancellation_Unlocking = 360,
        Cancelled = 400,

        Rejection_Applying = 500,
        Rejection_Unlocking = 510,
        Rejected = 550
    };

    struct TColumnBuildInfo {
        TString ColumnName;
        Ydb::TypedValue DefaultFromLiteral;
        bool NotNull = false;
        TString FamilyName;

        TColumnBuildInfo(const TString& name, const TString& serializedLiteral, bool notNull, const TString& familyName)
            : ColumnName(name)
            , NotNull(notNull)
            , FamilyName(familyName)
        {
            Y_ABORT_UNLESS(DefaultFromLiteral.ParseFromString(serializedLiteral));
        }

        TColumnBuildInfo(const TString& name, const Ydb::TypedValue& defaultFromLiteral, bool notNull, const TString& familyName)
            : ColumnName(name)
            , DefaultFromLiteral(defaultFromLiteral)
            , NotNull(notNull)
            , FamilyName(familyName)
        {
        }

        void SerializeToProto(NKikimrIndexBuilder::TColumnBuildSetting* setting) const {
            setting->SetColumnName(ColumnName);
            setting->mutable_default_from_literal()->CopyFrom(DefaultFromLiteral);
            setting->SetNotNull(NotNull);
            setting->SetFamily(FamilyName);
        }
    };

    enum class EBuildKind : ui32 {
        BuildKindUnspecified = 0,
        BuildIndex = 10,
        BuildColumn = 20
    };

    TActorId CreateSender;
    ui64 SenderCookie = 0;

    TIndexBuildId Id;
    TString Uid;

    TPathId DomainPathId;
    TPathId TablePathId;
    NKikimrSchemeOp::EIndexType IndexType = NKikimrSchemeOp::EIndexTypeInvalid;

    EBuildKind BuildKind = EBuildKind::BuildKindUnspecified;

    TString IndexName;
    TVector<TString> IndexColumns;
    TVector<TString> DataColumns;

    TVector<TColumnBuildInfo> BuildColumns;

    TString ImplTablePath;
    NTableIndex::TTableColumns ImplTableColumns;
    NKikimrSchemeOp::TTableDescription ImplTableDescription;

    EState State = EState::Invalid;
    TString Issue;

    TSet<TActorId> Subscribers;

    bool CancelRequested = false;

    TTxId AlterMainTableTxId = TTxId();
    NKikimrScheme::EStatus AlterMainTableTxStatus = NKikimrScheme::StatusSuccess;
    bool AlterMainTableTxDone = false;

    TTxId LockTxId = TTxId();
    NKikimrScheme::EStatus LockTxStatus = NKikimrScheme::StatusSuccess;
    bool LockTxDone = false;

    TTxId InitiateTxId = TTxId();
    NKikimrScheme::EStatus InitiateTxStatus = NKikimrScheme::StatusSuccess;
    bool InitiateTxDone = false;

    TStepId SnapshotStep;
    TTxId SnapshotTxId;

    TTxId ApplyTxId = TTxId();
    NKikimrScheme::EStatus ApplyTxStatus = NKikimrScheme::StatusSuccess;
    bool ApplyTxDone = false;

    TTxId UnlockTxId = TTxId();
    NKikimrScheme::EStatus UnlockTxStatus = NKikimrScheme::StatusSuccess;
    bool UnlockTxDone = false;

    bool BillingEventIsScheduled = false;
    TDuration ReBillPeriod = TDuration::Seconds(10);

    struct TShardStatus {
        TSerializedTableRange Range;
        TString LastKeyAck;
        ui64 SeqNoRound = 0;

        NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus Status = NKikimrTxDataShard::TEvBuildIndexProgressResponse::INVALID;

        Ydb::StatusIds::StatusCode UploadStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        TString DebugMessage;

        TBillingStats Processed;
        TBillingStats Billed;

        TShardStatus(TSerializedTableRange range, TString lastKeyAck);

        TString ToString(TShardIdx shardIdx = InvalidShardIdx) const {
            TStringBuilder result;

            result << "TShardStatus {";

            if (shardIdx) {
                result << " ShardIdx: " << shardIdx;
            }
            result << " Status: " << NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus_Name(Status);
            result << " UploadStatus: " << Ydb::StatusIds::StatusCode_Name(UploadStatus);
            result << " DebugMessage: " << DebugMessage;
            result << " SeqNoRound: " << SeqNoRound;
            result << " Processed: " << Processed.ToString();
            result << " Billed: " << Billed.ToString();

            result << " }";

            return result;
        }
    };
    TMap<TShardIdx, TShardStatus> Shards;

    TDeque<TShardIdx> ToUploadShards;

    THashSet<TShardIdx> DoneShards;
    THashSet<TShardIdx> InProgressShards;

    TBillingStats Processed;
    TBillingStats Billed;

    TIndexBuildInfo(TIndexBuildId id, TString uid)
        : Id(id)
        , Uid(uid)
    {}

    template<class TRow>
    void AddBuildColumnInfo(const TRow& row){
        TString columnName = row.template GetValue<Schema::BuildColumnOperationSettings::ColumnName>();
        TString defaultFromLiteral = row.template GetValue<Schema::BuildColumnOperationSettings::DefaultFromLiteral>();
        bool notNull = row.template GetValue<Schema::BuildColumnOperationSettings::NotNull>();
        TString familyName = row.template GetValue<Schema::BuildColumnOperationSettings::FamilyName>();
        BuildColumns.push_back(TColumnBuildInfo(columnName, defaultFromLiteral, notNull, familyName));
    }

    template<class TRowSetType>
    void AddIndexColumnInfo(const TRowSetType& row) {

        TString columnName =
            row.template GetValue<Schema::IndexBuildColumns::ColumnName>();
        EIndexColumnKind columnKind =
            row.template GetValueOrDefault<Schema::IndexBuildColumns::ColumnKind>(
                EIndexColumnKind::KeyColumn);
        ui32 columnNo = row.template GetValue<Schema::IndexBuildColumns::ColumnNo>();

        Y_VERIFY_S(columnNo == (IndexColumns.size() + DataColumns.size()),
                   "Unexpected non contiguous column number# "
                       << columnNo << " indexColumns# "
                       << IndexColumns.size() << " dataColumns# "
                       << DataColumns.size());

        switch (columnKind) {
        case EIndexColumnKind::KeyColumn:
            IndexColumns.push_back(columnName);
            break;
        case EIndexColumnKind::DataColumn:
            DataColumns.push_back(columnName);
            break;
        default:
            Y_FAIL_S("Unknown column kind# " << (int)columnKind);
            break;
        }
    }

    template<class TRow>
    static TIndexBuildInfo::TPtr FromRow(const TRow& row) {
        TIndexBuildId id = row.template GetValue<Schema::IndexBuild::Id>();
        TString uid = row.template GetValue<Schema::IndexBuild::Uid>();

        TIndexBuildInfo::TPtr indexInfo = new TIndexBuildInfo(id, uid);

        indexInfo->DomainPathId =
            TPathId(row.template GetValue<Schema::IndexBuild::DomainOwnerId>(),
                    row.template GetValue<Schema::IndexBuild::DomainLocalId>());

        indexInfo->TablePathId =
            TPathId(row.template GetValue<Schema::IndexBuild::TableOwnerId>(),
                    row.template GetValue<Schema::IndexBuild::TableLocalId>());

        indexInfo->IndexName = row.template GetValue<Schema::IndexBuild::IndexName>();
        indexInfo->IndexType = row.template GetValue<Schema::IndexBuild::IndexType>();

        indexInfo->State = TIndexBuildInfo::EState(
            row.template GetValue<Schema::IndexBuild::State>());
        indexInfo->Issue =
            row.template GetValueOrDefault<Schema::IndexBuild::Issue>();
        indexInfo->CancelRequested =
            row.template GetValueOrDefault<Schema::IndexBuild::CancelRequest>(false);

        indexInfo->LockTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxId>(
                indexInfo->LockTxId);
        indexInfo->LockTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxStatus>(
                indexInfo->LockTxStatus);
        indexInfo->LockTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxDone>(
                indexInfo->LockTxDone);

        indexInfo->InitiateTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxId>(
                indexInfo->InitiateTxId);
        indexInfo->InitiateTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxStatus>(
                indexInfo->InitiateTxStatus);
        indexInfo->InitiateTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxDone>(
                indexInfo->InitiateTxDone);

        indexInfo->Limits.MaxBatchRows =
            row.template GetValue<Schema::IndexBuild::MaxBatchRows>();
        indexInfo->Limits.MaxBatchBytes =
            row.template GetValue<Schema::IndexBuild::MaxBatchBytes>();
        indexInfo->Limits.MaxShards =
            row.template GetValue<Schema::IndexBuild::MaxShards>();
        indexInfo->Limits.MaxRetries =
            row.template GetValueOrDefault<Schema::IndexBuild::MaxRetries>(
                indexInfo->Limits.MaxRetries);

        indexInfo->ApplyTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxId>(
                indexInfo->ApplyTxId);
        indexInfo->ApplyTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxStatus>(
                indexInfo->ApplyTxStatus);
        indexInfo->ApplyTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxDone>(
                indexInfo->ApplyTxDone);

        indexInfo->UnlockTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxId>(
                indexInfo->UnlockTxId);
        indexInfo->UnlockTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxStatus>(
                indexInfo->UnlockTxStatus);
        indexInfo->UnlockTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxDone>(
                indexInfo->UnlockTxDone);

        // note: please note that here we specify BuildIndex as operation
        // default, because previosly this table was dedicated for build index
        // operations only.
        indexInfo->BuildKind = TIndexBuildInfo::EBuildKind(
            row.template GetValueOrDefault<Schema::IndexBuild::BuildKind>(
                ui32(TIndexBuildInfo::EBuildKind::BuildIndex)));

        indexInfo->AlterMainTableTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxId>(
                indexInfo->AlterMainTableTxId);
        indexInfo->AlterMainTableTxStatus =
            row
                .template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxStatus>(
                    indexInfo->AlterMainTableTxStatus);
        indexInfo->AlterMainTableTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxDone>(
                indexInfo->AlterMainTableTxDone);

        indexInfo->Billed = TBillingStats(
            row.template GetValueOrDefault<Schema::IndexBuild::RowsBilled>(0),
            row.template GetValueOrDefault<Schema::IndexBuild::BytesBilled>(0));

        return indexInfo;
    }

    template<class TRow>
    void AddShardStatus(const TRow& row) {
        TShardIdx shardIdx =
            TShardIdx(row.template GetValue<
                          Schema::IndexBuildShardStatus::OwnerShardIdx>(),
                      row.template GetValue<
                          Schema::IndexBuildShardStatus::LocalShardIdx>());

        NKikimrTx::TKeyRange range =
            row.template GetValue<Schema::IndexBuildShardStatus::Range>();
        TString lastKeyAck =
            row.template GetValue<Schema::IndexBuildShardStatus::LastKeyAck>();

        Shards.emplace(
            shardIdx, TIndexBuildInfo::TShardStatus(
                          TSerializedTableRange(range), std::move(lastKeyAck)));
        TIndexBuildInfo::TShardStatus &shardStatus = Shards.at(shardIdx);

        shardStatus.Status =
            row.template GetValue<Schema::IndexBuildShardStatus::Status>();

        shardStatus.DebugMessage = row.template GetValueOrDefault<
            Schema::IndexBuildShardStatus::Message>();
        shardStatus.UploadStatus = row.template GetValueOrDefault<
            Schema::IndexBuildShardStatus::UploadStatus>(
            Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);

        shardStatus.Processed = TBillingStats(
            row.template GetValueOrDefault<
                Schema::IndexBuildShardStatus::RowsProcessed>(0),
            row.template GetValueOrDefault<
                Schema::IndexBuildShardStatus::BytesProcessed>(0));

        Processed += shardStatus.Processed;
    }

    bool IsCancellationRequested() const {
        return CancelRequested;
    }

    bool IsBuildIndex() const {
        return BuildKind == EBuildKind::BuildIndex;
    }

    bool IsBuildColumn() const {
        return BuildKind == EBuildKind::BuildColumn;
    }

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled || State == EState::Rejected;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    void AddNotifySubscriber(const TActorId& actorID) {
        Y_ABORT_UNLESS(!IsFinished());
        Subscribers.insert(actorID);
    }

    float CalcProgressPercent() const {
        if (Shards) {
            float totalShards = Shards.size();
            return 100.0 * DoneShards.size() / totalShards;
        }
        // No shards - no progress
        return 0.0;
    }

    void SerializeToProto(TSchemeShard* ss, NKikimrIndexBuilder::TColumnBuildSettings* to) const;
    void SerializeToProto(TSchemeShard* ss, NKikimrSchemeOp::TIndexBuildConfig* to) const;

};

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
};

struct TViewInfo : TSimpleRefCount<TViewInfo> {
    using TPtr = TIntrusivePtr<TViewInfo>;

    ui64 AlterVersion = 0;
    TString QueryText;
};

struct TResourcePoolInfo : TSimpleRefCount<TResourcePoolInfo> {
    using TPtr = TIntrusivePtr<TResourcePoolInfo>;

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TResourcePoolProperties Properties;
};

bool ValidateTtlSettings(const NKikimrSchemeOp::TTTLSettings& ttl,
    const THashMap<ui32, TTableInfo::TColumn>& sourceColumns,
    const THashMap<ui32, TTableInfo::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    const TSubDomainInfo& subDomain, TString& errStr);
}

}

template <>
inline void Out<NKikimr::NSchemeShard::TIndexBuildInfo::TShardStatus>
    (IOutputStream& o, const NKikimr::NSchemeShard::TIndexBuildInfo::TShardStatus& info)
{
    o << info.ToString();
}

template <>
inline void Out<NKikimr::NSchemeShard::TBillingStats>
    (IOutputStream& o, const NKikimr::NSchemeShard::TBillingStats& stats)
{
    o << stats.ToString();
}

template <>
inline void Out<NKikimr::NSchemeShard::TIndexBuildInfo>
    (IOutputStream& o, const NKikimr::NSchemeShard::TIndexBuildInfo& info)
{

    o << "TBuildInfo{";
    o << " IndexBuildId: " << info.Id;
    o << ", Uid: " << info.Uid;
    o << ", DomainPathId: " << info.DomainPathId;
    o << ", TablePathId: " << info.TablePathId;
    o << ", IndexType: " << NKikimrSchemeOp::EIndexType_Name(info.IndexType);
    o << ", IndexName: " << info.IndexName;
    for (const auto& x: info.IndexColumns) {
        o << ", IndexColumn: " << x;
    }
    for (const auto& x: info.DataColumns) {
        o << ", DataColumns: " << x;
    }

    o << ", State: " << info.State;
    o << ", IsCancellationRequested: " << info.CancelRequested;

    o << ", Issue: " << info.Issue;
    o << ", SubscribersCount: " << info.Subscribers.size();

    o << ", CreateSender: " << info.CreateSender.ToString();

    o << ", AlterMainTableTxId: " << info.AlterMainTableTxId;
    o << ", AlterMainTableTxStatus: " <<  NKikimrScheme::EStatus_Name(info.AlterMainTableTxStatus);
    o << ", AlterMainTableTxDone: " << info.AlterMainTableTxDone;

    o << ", LockTxId: " << info.LockTxId;
    o << ", LockTxStatus: " << NKikimrScheme::EStatus_Name(info.LockTxStatus);
    o << ", LockTxDone: " << info.LockTxDone;

    o << ", InitiateTxId: " << info.InitiateTxId;
    o << ", InitiateTxStatus: " << NKikimrScheme::EStatus_Name(info.InitiateTxStatus);
    o << ", InitiateTxDone: " << info.InitiateTxDone;

    o << ", SnapshotStepId: " << info.SnapshotStep;

    o << ", ApplyTxId: " << info.ApplyTxId;
    o << ", ApplyTxStatus: " << NKikimrScheme::EStatus_Name(info.ApplyTxStatus);
    o << ", ApplyTxDone: " << info.ApplyTxDone;

    o << ", UnlockTxId: " << info.UnlockTxId;
    o << ", UnlockTxStatus: " << NKikimrScheme::EStatus_Name(info.UnlockTxStatus);
    o << ", UnlockTxDone: " << info.UnlockTxDone;

    o << ", ToUploadShards: " << info.ToUploadShards.size();
    o << ", DoneShards: " << info.DoneShards.size();

    for (const auto& x: info.InProgressShards) {
        o << ", ShardsInProgress: " << x;
    }

    o << ", Processed: " << info.Processed;
    o << ", Billed: " << info.Billed;

    o << "}";
}
