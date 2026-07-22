#include "schemeshard_impl.h"
#include "schemeshard__local_index_migration.h"
#include "schemeshard_svp_migration.h"

#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"
#include "olap/operations/local_index_helpers.h"
#include "schemeshard.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_svp_migration.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/resolver.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_sysviews_update.h>

#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/password_checker/password_checker.h>

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/random/random.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {

// Both Schema::TablePartitionStats and Schema::TablePartitionStatsByShardIdx
// expose identically-named columns; templating on the table type lets us write
// the field block once.
template <typename T, typename TRow>
void WritePartitionStatsRow(TRow& row, const TPartitionStats& stats) {
    row.Update(
        NIceDb::TUpdate<typename T::SeqNoGeneration>(stats.SeqNo.Generation),
        NIceDb::TUpdate<typename T::SeqNoRound>(stats.SeqNo.Round),

        NIceDb::TUpdate<typename T::RowCount>(stats.RowCount),
        NIceDb::TUpdate<typename T::DataSize>(stats.DataSize),
        NIceDb::TUpdate<typename T::IndexSize>(stats.IndexSize),
        NIceDb::TUpdate<typename T::ByKeyFilterSize>(stats.ByKeyFilterSize),

        NIceDb::TUpdate<typename T::LastAccessTime>(stats.LastAccessTime.GetValue()),
        NIceDb::TUpdate<typename T::LastUpdateTime>(stats.LastUpdateTime.GetValue()),

        NIceDb::TUpdate<typename T::ImmediateTxCompleted>(stats.ImmediateTxCompleted),
        NIceDb::TUpdate<typename T::PlannedTxCompleted>(stats.PlannedTxCompleted),
        NIceDb::TUpdate<typename T::TxRejectedByOverload>(stats.TxRejectedByOverload),
        NIceDb::TUpdate<typename T::TxRejectedBySpace>(stats.TxRejectedBySpace),
        NIceDb::TUpdate<typename T::TxCompleteLag>(stats.TxCompleteLag.GetValue()),
        NIceDb::TUpdate<typename T::InFlightTxCount>(stats.InFlightTxCount),

        NIceDb::TUpdate<typename T::RowUpdates>(stats.RowUpdates),
        NIceDb::TUpdate<typename T::RowDeletes>(stats.RowDeletes),
        NIceDb::TUpdate<typename T::RowReads>(stats.RowReads),
        NIceDb::TUpdate<typename T::RangeReads>(stats.RangeReads),
        NIceDb::TUpdate<typename T::RangeReadRows>(stats.RangeReadRows),

        NIceDb::TUpdate<typename T::CPU>(stats.GetCurrentRawCpuUsage()),
        NIceDb::TUpdate<typename T::Memory>(stats.Memory),
        NIceDb::TUpdate<typename T::Network>(stats.Network),
        NIceDb::TUpdate<typename T::Storage>(stats.Storage),
        NIceDb::TUpdate<typename T::ReadThroughput>(stats.ReadThroughput),
        NIceDb::TUpdate<typename T::WriteThroughput>(stats.WriteThroughput),
        NIceDb::TUpdate<typename T::ReadIops>(stats.ReadIops),
        NIceDb::TUpdate<typename T::WriteIops>(stats.WriteIops),

        NIceDb::TUpdate<typename T::SearchHeight>(stats.SearchHeight),
        NIceDb::TUpdate<typename T::FullCompactionTs>(stats.FullCompactionTs),
        NIceDb::TUpdate<typename T::MemDataSize>(stats.MemDataSize),

        NIceDb::TUpdate<typename T::LocksAcquired>(stats.LocksAcquired),
        NIceDb::TUpdate<typename T::LocksWholeShard>(stats.LocksWholeShard),
        NIceDb::TUpdate<typename T::LocksBroken>(stats.LocksBroken)
    );

    if (!stats.StoragePoolsStats.empty()) {
        NKikimrTableStats::TStoragePoolsStats proto;
        for (const auto& [poolKind, poolStats] : stats.StoragePoolsStats) {
            auto* poolUsage = proto.MutablePoolsUsage()->Add();
            poolUsage->SetPoolKind(poolKind);
            poolUsage->SetDataSize(poolStats.DataSize);
            poolUsage->SetIndexSize(poolStats.IndexSize);
        }
        TString serialized;
        Y_ABORT_UNLESS(proto.SerializeToString(&serialized));
        row.Update(NIceDb::TUpdate<typename T::StoragePoolsStats>(serialized));
    } else {
        row.Update(NIceDb::TNull<typename T::StoragePoolsStats>());
    }
}

} // namespace

void TSchemeShard::PersistTablePartitionStatsByPosition(NIceDb::TNiceDb& db, const TPathId& tableId, ui64 partitionId, const TPartitionStats& stats) {
    using T = Schema::TablePartitionStats;
    auto row = db.Table<T>().Key(tableId.OwnerId, tableId.LocalPathId, partitionId);
    WritePartitionStatsRow<T>(row, stats);
}

void TSchemeShard::PersistTablePartitionStatsByShardIdx(NIceDb::TNiceDb& db, const TPathId& tableId, const TShardIdx& shardIdx, const TPartitionStats& stats) {
    using T = Schema::TablePartitionStatsByShardIdx;
    auto row = db.Table<T>().Key(tableId.OwnerId, tableId.LocalPathId, shardIdx.GetOwnerId(), shardIdx.GetLocalId());
    WritePartitionStatsRow<T>(row, stats);
}

void TSchemeShard::PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TShardIdx& shardIdx, const TTableInfo::TPtr tableInfo) {
    if (!AppData()->FeatureFlags.GetEnablePersistentPartitionStats()) {
        return;
    }

    const TTableShardInfo* p = tableInfo->GetPartitionStore().FindPtr(shardIdx);
    if (!p) {
        return;
    }

    const auto& tableStats = tableInfo->GetStats();
    const auto* statsPtr = tableStats.PartitionStats.FindPtr(shardIdx);
    if (!statsPtr) {
        return;
    }

    if (tableInfo->PartitionsInShardIdxFormat) {
        PersistTablePartitionStatsByShardIdx(db, tableId, shardIdx, *statsPtr);
    } else {
        PersistTablePartitionStatsByPosition(db, tableId, p->Position, *statsPtr);
    }
}

void TSchemeShard::PersistAllTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TTableInfo::TPtr tableInfo, ui64 startIdx) {
    if (!AppData()->FeatureFlags.GetEnablePersistentPartitionStats()) {
        return;
    }

    const auto& tableStats = tableInfo->GetStats();
    const auto& partitions = tableInfo->GetPartitions();
    const bool byShardIdx = tableInfo->PartitionsInShardIdxFormat;

    for (ui64 pi = startIdx; pi < partitions.size(); ++pi) {
        const TShardIdx shardIdx = partitions[pi]->ShardIdx;
        const auto* statsPtr = tableStats.PartitionStats.FindPtr(shardIdx);
        if (!statsPtr) {
            continue;
        }
        if (byShardIdx) {
            PersistTablePartitionStatsByShardIdx(db, tableId, shardIdx, *statsPtr);
        } else {
            PersistTablePartitionStatsByPosition(db, tableId, pi, *statsPtr);
        }
    }
}

void TSchemeShard::PersistPersQueueGroupStats(NIceDb::TNiceDb &db, const TPathId pathId, const TTopicStats& stats) {
    db.Table<Schema::PersQueueGroupStats>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroupStats::SeqNoGeneration>(stats.SeqNo.Generation),
        NIceDb::TUpdate<Schema::PersQueueGroupStats::SeqNoRound>(stats.SeqNo.Round),

        NIceDb::TUpdate<Schema::PersQueueGroupStats::DataSize>(stats.DataSize),
        NIceDb::TUpdate<Schema::PersQueueGroupStats::UsedReserveSize>(stats.UsedReserveSize)
    );
}

void TSchemeShard::PersistTableAlterVersion(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::AlterVersion>(tableInfo->AlterVersion));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::AlterVersion>(tableInfo->AlterVersion));
    }
}

void TSchemeShard::PersistClearAlterTableFull(NIceDb::TNiceDb& db, const TPathId& pathId) {
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::AlterTableFull>(TString()));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(TString()));
    }
}

void TSchemeShard::PersistTableFinishColumnBuilding(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 colId) {
    const auto& cinfo = tableInfo->Columns.at(colId);
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Columns>().Key(pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::Columns::IsBuildInProgress>(cinfo.IsBuildInProgress));

    } else {
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress));
    }
}

void TSchemeShard::PersistTableIsRestore(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::IsRestore>(tableInfo->IsRestore));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::IsRestore>(tableInfo->IsRestore));
    }
}

void TSchemeShard::PersistTableIsRestore(NIceDb::TNiceDb& db, const TPathId pathId, const TColumnTableInfo::TPtr tableInfo) {
    db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ColumnTables::IsRestore>(tableInfo->IsRestore));
}

void TSchemeShard::PersistTableAltered(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    TString partitionConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->PartitionConfig().SerializeToString(&partitionConfig);

    TString ttlSettings;
    if (tableInfo->HasTTLSettings()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->TTLSettings().SerializeToString(&ttlSettings);
    }

    TString detailedMetricsSettings;
    if (tableInfo->HasDetailedMetricsSettings()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->GetDetailedMetricsSettings()
            .SerializeToString(&detailedMetricsSettings);
    }

    TString replicationConfig;
    if (tableInfo->HasReplicationConfig()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->ReplicationConfig().SerializeToString(&replicationConfig);
    }

    TString incrementalBackupConfig;
    if (tableInfo->HasIncrementalBackupConfig()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->IncrementalBackupConfig().SerializeToString(&incrementalBackupConfig);
    }

    TString statistics;
    if (tableInfo->HasMultiColumnStatistics()) {
        NKikimrSchemeOp::TTableDescription statisticsHolder;
        statisticsHolder.MutableMultiColumnStatistics()->CopyFrom(tableInfo->MultiColumnStatistics());
        Y_PROTOBUF_SUPPRESS_NODISCARD statisticsHolder.SerializeToString(&statistics);
    }

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::NextColId>(tableInfo->NextColumnId),
            NIceDb::TUpdate<Schema::Tables::PartitionConfig>(partitionConfig),
            NIceDb::TUpdate<Schema::Tables::AlterVersion>(tableInfo->AlterVersion),
            NIceDb::TUpdate<Schema::Tables::AlterTable>(TString()),
            NIceDb::TUpdate<Schema::Tables::AlterTableFull>(TString()),
            NIceDb::TUpdate<Schema::Tables::TTLSettings>(ttlSettings),
            NIceDb::TUpdate<Schema::Tables::IsBackup>(tableInfo->IsBackup),
            NIceDb::TUpdate<Schema::Tables::IsRestore>(tableInfo->IsRestore),
            NIceDb::TUpdate<Schema::Tables::ReplicationConfig>(replicationConfig),
            NIceDb::TUpdate<Schema::Tables::IsTemporary>(tableInfo->IsTemporary),
            NIceDb::TUpdate<Schema::Tables::OwnerActorId>(tableInfo->OwnerActorId.ToString()),
            NIceDb::TUpdate<Schema::Tables::IncrementalBackupConfig>(incrementalBackupConfig),
            NIceDb::TUpdate<Schema::Tables::DetailedMetricsSettings>(detailedMetricsSettings),
            NIceDb::TUpdate<Schema::Tables::MultiColumnStatistics>(statistics)
        );
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::NextColId>(tableInfo->NextColumnId),
            NIceDb::TUpdate<Schema::MigratedTables::PartitionConfig>(partitionConfig),
            NIceDb::TUpdate<Schema::MigratedTables::AlterVersion>(tableInfo->AlterVersion),
            NIceDb::TUpdate<Schema::MigratedTables::AlterTable>(TString()),
            NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(TString()),
            NIceDb::TUpdate<Schema::MigratedTables::TTLSettings>(ttlSettings),
            NIceDb::TUpdate<Schema::MigratedTables::IsBackup>(tableInfo->IsBackup),
            NIceDb::TUpdate<Schema::MigratedTables::IsRestore>(tableInfo->IsRestore),
            NIceDb::TUpdate<Schema::MigratedTables::ReplicationConfig>(replicationConfig),
            NIceDb::TUpdate<Schema::MigratedTables::IsTemporary>(tableInfo->IsTemporary),
            NIceDb::TUpdate<Schema::MigratedTables::OwnerActorId>(tableInfo->OwnerActorId.ToString()),
            NIceDb::TUpdate<Schema::MigratedTables::IncrementalBackupConfig>(incrementalBackupConfig),
            NIceDb::TUpdate<Schema::MigratedTables::DetailedMetricsSettings>(detailedMetricsSettings),
            NIceDb::TUpdate<Schema::MigratedTables::MultiColumnStatistics>(statistics)
        );
    }

    for (auto col : tableInfo->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::Columns>().Key(pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::Columns::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::Columns::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::Columns::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::Columns::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::Columns::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::Columns::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::Columns::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::Columns::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::Columns::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::Columns::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::Columns::IsBuildInProgress>(cinfo.IsBuildInProgress),
                NIceDb::TUpdate<Schema::Columns::SetNotNullInProgress>(cinfo.SetNotNullInProgress));

            db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, colId).Delete();
        } else {
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::MigratedColumns::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::MigratedColumns::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::MigratedColumns::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::MigratedColumns::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::MigratedColumns::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::MigratedColumns::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::MigratedColumns::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::MigratedColumns::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::MigratedColumns::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::MigratedColumns::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress),
                NIceDb::TUpdate<Schema::MigratedColumns::SetNotNullInProgress>(cinfo.SetNotNullInProgress));
        }
        db.Table<Schema::MigratedColumnAlters>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Delete();
    }
}

/// @note Legacy. It's better to use Alter logic here: save new data in AlterData and swap it on complete.
void TSchemeShard::PersistTableCreated(NIceDb::TNiceDb& db, const TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Tables::AlterVersion>(1));
}

void TSchemeShard::PersistAddAlterTable(NIceDb::TNiceDb& db, TPathId pathId, const TTableInfo::TAlterDataPtr alter) {
    TString proto;
    Y_PROTOBUF_SUPPRESS_NODISCARD alter->TableDescriptionFull->SerializeToString(&proto);
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::AlterTableFull>(proto));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(proto));
    }

    for (auto col : alter->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::ColumnAlters::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::ColumnAlters::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::ColumnAlters::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::ColumnAlters::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::ColumnAlters::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::ColumnAlters::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::ColumnAlters::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::ColumnAlters::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::ColumnAlters::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::ColumnAlters::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::ColumnAlters::IsBuildInProgress>(cinfo.IsBuildInProgress),
                NIceDb::TUpdate<Schema::ColumnAlters::SetNotNullInProgress>(cinfo.SetNotNullInProgress));
        } else {
            db.Table<Schema::MigratedColumnAlters>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::IsBuildInProgress>(cinfo.IsBuildInProgress),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::SetNotNullInProgress>(cinfo.SetNotNullInProgress));
        }
    }
}

void TSchemeShard::PersistPersQueueGroup(NIceDb::TNiceDb& db, TPathId pathId, const TTopicInfo::TPtr pqGroup) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroups>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroups::TabletConfig>(pqGroup->TabletConfig),
        NIceDb::TUpdate<Schema::PersQueueGroups::MaxPQPerShard>(pqGroup->MaxPartsPerTablet),
        NIceDb::TUpdate<Schema::PersQueueGroups::AlterVersion>(pqGroup->AlterVersion),
        NIceDb::TUpdate<Schema::PersQueueGroups::TotalGroupCount>(pqGroup->TotalGroupCount),
        NIceDb::TUpdate<Schema::PersQueueGroups::NextPartitionId>(pqGroup->NextPartitionId));
}

void TSchemeShard::PersistRemovePersQueueGroup(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = Topics.find(pathId);
    if (it != Topics.end()) {
        TTopicInfo::TPtr pqGroup = it->second;

        if (pqGroup->AlterData) {
            PersistRemovePersQueueGroupAlter(db, pathId);
        }

        for (const auto& shard : pqGroup->Shards) {
            for (const auto& pqInfo : shard.second->Partitions) {
                PersistRemovePersQueue(db, pathId, pqInfo->PqId);
            }
        }

        Topics.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::PersQueueGroups>().Key(pathId.LocalPathId).Delete();
    db.Table<Schema::PersQueueGroupStats>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistAddPersQueueGroupAlter(NIceDb::TNiceDb& db, TPathId pathId, const TTopicInfo::TPtr alterData) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroupAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::TabletConfig>(alterData->TabletConfig),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::MaxPQPerShard>(alterData->MaxPartsPerTablet),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::TotalGroupCount>(alterData->TotalGroupCount),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::NextPartitionId>(alterData->NextPartitionId),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::BootstrapConfig>(alterData->BootstrapConfig));
}

void TSchemeShard::PersistRemovePersQueueGroupAlter(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroupAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistPersQueue(NIceDb::TNiceDb &db, TPathId pathId, TShardIdx shardIdx, const TTopicTabletInfo::TTopicPartitionInfo& partitionInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    Y_ABORT_UNLESS(partitionInfo.ParentPartitionIds.size() <= 2);
    auto it = partitionInfo.ParentPartitionIds.begin();
    const auto parent = it != partitionInfo.ParentPartitionIds.end() ? *(it++) : Max<ui32>();
    const auto adjacentParent = it != partitionInfo.ParentPartitionIds.end() ? *(it++) : Max<ui32>();

    db.Table<Schema::PersQueues>()
        .Key(pathId.LocalPathId, partitionInfo.PqId)
        .Update(NIceDb::TUpdate<Schema::PersQueues::ShardIdx>(shardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::PersQueues::GroupId>(partitionInfo.GroupId),
                NIceDb::TUpdate<Schema::PersQueues::AlterVersion>(partitionInfo.AlterVersion),
                NIceDb::TUpdate<Schema::PersQueues::CreateVersion>(partitionInfo.CreateVersion),
                NIceDb::TUpdate<Schema::PersQueues::Status>(partitionInfo.Status),
                NIceDb::TUpdate<Schema::PersQueues::Parent>(parent),
                NIceDb::TUpdate<Schema::PersQueues::AdjacentParent>(adjacentParent));

    if (partitionInfo.CreationTimestamp) {
        db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, partitionInfo.PqId).Update(
            NIceDb::TUpdate<Schema::PersQueues::CreationTimestampSeconds>(partitionInfo.CreationTimestamp.Seconds()));
    }

    if (partitionInfo.KeyRange) {
        if (partitionInfo.KeyRange->FromBound) {
            db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, partitionInfo.PqId).Update(
                NIceDb::TUpdate<Schema::PersQueues::RangeBegin>(*partitionInfo.KeyRange->FromBound));
        }

        if (partitionInfo.KeyRange->ToBound) {
            db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, partitionInfo.PqId).Update(
                NIceDb::TUpdate<Schema::PersQueues::RangeEnd>(*partitionInfo.KeyRange->ToBound));
        }
    }
}

void TSchemeShard::PersistRemovePersQueue(NIceDb::TNiceDb &db, TPathId pathId, ui32 pqId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, pqId).Delete();
}

void TSchemeShard::PersistRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId, const TRtmrVolumeInfo::TPtr rtmrVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::RtmrVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::RtmrVolumes::PartitionsCount>(rtmrVol->Partitions.size()));

    for (const auto& partition: rtmrVol->Partitions) {
        TString partitionId = TString((const char*)partition.second->Id.dw, sizeof(TGUID));

        db.Table<Schema::RTMRPartitions>().Key(pathId.LocalPathId, partition.second->ShardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::RTMRPartitions::PartitionId>(partitionId),
            NIceDb::TUpdate<Schema::RTMRPartitions::BusKey>(partition.second->BusKey));
    }
}

void TSchemeShard::PersistExternalTable(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    const auto it = ExternalTables.find(pathId);
    Y_ABORT_UNLESS(it != ExternalTables.end());
    const auto info = it->second;
    Y_ABORT_UNLESS(info);
    PersistExternalTable(db, pathId, info);
}

void TSchemeShard::PersistExternalTable(NIceDb::TNiceDb &db, TPathId pathId, const TExternalTableInfo::TPtr externalTableInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ExternalTable>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ExternalTable::SourceType>{externalTableInfo->SourceType},
        NIceDb::TUpdate<Schema::ExternalTable::DataSourcePath>{externalTableInfo->DataSourcePath},
        NIceDb::TUpdate<Schema::ExternalTable::Location>{externalTableInfo->Location},
        NIceDb::TUpdate<Schema::ExternalTable::AlterVersion>{externalTableInfo->AlterVersion},
        NIceDb::TUpdate<Schema::ExternalTable::Content>{externalTableInfo->Content});

    for (auto col : externalTableInfo->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::MigratedColumns::ColName>(cinfo.Name),
            NIceDb::TUpdate<Schema::MigratedColumns::ColType>((ui32)columnType.TypeId),
            NIceDb::TUpdate<Schema::MigratedColumns::ColTypeData>(typeData),
            NIceDb::TUpdate<Schema::MigratedColumns::ColKeyOrder>(cinfo.KeyOrder),
            NIceDb::TUpdate<Schema::MigratedColumns::CreateVersion>(cinfo.CreateVersion),
            NIceDb::TUpdate<Schema::MigratedColumns::DeleteVersion>(cinfo.DeleteVersion),
            NIceDb::TUpdate<Schema::MigratedColumns::Family>(cinfo.Family),
            NIceDb::TUpdate<Schema::MigratedColumns::DefaultKind>(cinfo.DefaultKind),
            NIceDb::TUpdate<Schema::MigratedColumns::DefaultValue>(cinfo.DefaultValue),
            NIceDb::TUpdate<Schema::MigratedColumns::NotNull>(cinfo.NotNull),
            NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress),
            NIceDb::TUpdate<Schema::MigratedColumns::SetNotNullInProgress>(cinfo.SetNotNullInProgress));
    }
}

void TSchemeShard::PersistRemoveExternalTable(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (ExternalTables.contains(pathId)) {
        auto externalTableInfo = ExternalTables.at(pathId);

        for (auto col : externalTableInfo->Columns) {
            const ui32 colId = col.first;
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Delete();
        }

        ExternalTables.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::ExternalTable>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistExternalDataSource(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    const auto it = ExternalDataSources.find(pathId);
    Y_ABORT_UNLESS(it != ExternalDataSources.end());
    const auto info = it->second;
    Y_ABORT_UNLESS(info);
    PersistExternalDataSource(db, pathId, info);
}

void TSchemeShard::PersistExternalDataSource(NIceDb::TNiceDb &db, TPathId pathId, const TExternalDataSourceInfo::TPtr externalDataSourceInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ExternalDataSource>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ExternalDataSource::AlterVersion>{externalDataSourceInfo->AlterVersion},
        NIceDb::TUpdate<Schema::ExternalDataSource::SourceType>{externalDataSourceInfo->SourceType},
        NIceDb::TUpdate<Schema::ExternalDataSource::Location>{externalDataSourceInfo->Location},
        NIceDb::TUpdate<Schema::ExternalDataSource::Installation>{externalDataSourceInfo->Installation},
        NIceDb::TUpdate<Schema::ExternalDataSource::Auth>{externalDataSourceInfo->Auth.SerializeAsString()},
        NIceDb::TUpdate<Schema::ExternalDataSource::ExternalTableReferences>{externalDataSourceInfo->ExternalTableReferences.SerializeAsString()},
        NIceDb::TUpdate<Schema::ExternalDataSource::Properties>{externalDataSourceInfo->Properties.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveExternalDataSource(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (ExternalDataSources.contains(pathId)) {
        ExternalDataSources.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::ExternalDataSource>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistExternalDataSourceReference(NIceDb::TNiceDb& db, TPathId pathId, const TPath& referrer) {
    auto findSource = ExternalDataSources.FindPtr(pathId);
    Y_ABORT_UNLESS(findSource);
    auto* ref = (*findSource)->ExternalTableReferences.AddReferences();
    ref->SetPath(referrer.PathString());
    referrer->PathId.ToProto(ref->MutablePathId());
    db.Table<Schema::ExternalDataSource>()
        .Key(pathId.OwnerId, pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::ExternalDataSource::ExternalTableReferences>{ (*findSource)->ExternalTableReferences.SerializeAsString() });
}

void TSchemeShard::PersistRemoveExternalDataSourceReference(NIceDb::TNiceDb& db, TPathId pathId, TPathId referrer) {
    auto findSource = ExternalDataSources.FindPtr(pathId);
    Y_ABORT_UNLESS(findSource);
    EraseIf(*(*findSource)->ExternalTableReferences.MutableReferences(),
        [referrer](const NKikimrSchemeOp::TExternalTableReferences::TReference& reference) {
            return TPathId::FromProto(reference.GetPathId()) == referrer;
        });
    db.Table<Schema::ExternalDataSource>()
        .Key(pathId.OwnerId, pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::ExternalDataSource::ExternalTableReferences>{ (*findSource)->ExternalTableReferences.SerializeAsString() });
}

void TSchemeShard::PersistView(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto path = PathsById.find(pathId);
    Y_ABORT_UNLESS(path != PathsById.end());
    Y_ABORT_UNLESS(path->second && path->second->IsView());

    const auto view = Views.find(pathId);
    Y_ABORT_UNLESS(view != Views.end());
    TViewInfo::TPtr viewInfo = view->second;
    Y_ABORT_UNLESS(viewInfo);

    db.Table<Schema::View>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::View::AlterVersion>{viewInfo->AlterVersion},
        NIceDb::TUpdate<Schema::View::QueryText>{viewInfo->QueryText},
        NIceDb::TUpdate<Schema::View::CapturedContext>{viewInfo->CapturedContext.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveView(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (const auto view = Views.find(pathId); view != Views.end()) {
        Views.erase(view);
    }
    db.Table<Schema::View>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSysView(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto path = PathsById.find(pathId);
    Y_ABORT_UNLESS(path != PathsById.end());
    Y_ABORT_UNLESS(path->second && path->second->IsSysView());

    const auto sysView = SysViews.find(pathId);
    Y_ABORT_UNLESS(sysView != SysViews.end());
    TSysViewInfo::TPtr sysViewInfo = sysView->second;
    Y_ABORT_UNLESS(sysViewInfo);

    db.Table<Schema::SysView>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SysView::AlterVersion>{sysViewInfo->AlterVersion},
        NIceDb::TUpdate<Schema::SysView::SysViewType>{sysViewInfo->Type}
    );
}

void TSchemeShard::PersistRemoveSysView(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (const auto sysView = SysViews.find(pathId); sysView != SysViews.end()) {
        SysViews.erase(sysView);
    }

    db.Table<Schema::SysView>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistResourcePool(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    const auto it = ResourcePools.find(pathId);
    Y_ABORT_UNLESS(it != ResourcePools.end());
    const auto info = it->second;
    Y_ABORT_UNLESS(info);
    PersistResourcePool(db, pathId, info);
}

void TSchemeShard::PersistResourcePool(NIceDb::TNiceDb& db, TPathId pathId, const TResourcePoolInfo::TPtr resourcePool) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ResourcePool>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ResourcePool::AlterVersion>{resourcePool->AlterVersion},
        NIceDb::TUpdate<Schema::ResourcePool::Properties>{resourcePool->Properties.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveResourcePool(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (ResourcePools.contains(pathId)) {
        ResourcePools.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::ResourcePool>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistBackupCollection(NIceDb::TNiceDb& db, TPathId pathId, const TBackupCollectionInfo::TPtr backupCollection) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::BackupCollection>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BackupCollection::AlterVersion>{backupCollection->AlterVersion},
        NIceDb::TUpdate<Schema::BackupCollection::Description>{backupCollection->Description.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveBackupCollection(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (BackupCollections.contains(pathId)) {
        UnregisterBackupCollectionTables(BackupCollections[pathId]);
        BackupCollections.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::BackupCollection>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::RegisterBackupCollectionTables(const TBackupCollectionInfo::TPtr& collection) {
    if (!collection) return;
    const auto& desc = collection->Description;
    for (const auto& entry : desc.GetExplicitEntryList().GetEntries()) {
        TPath path = TPath::Resolve(entry.GetPath(), this);
        if (path.IsResolved() && path->IsTable()) {
            TableInBackupCollections.insert(path.Base()->PathId);
        }
    }
}

void TSchemeShard::UnregisterBackupCollectionTables(const TBackupCollectionInfo::TPtr& collection) {
    if (!collection) return;
    const auto& desc = collection->Description;
    for (const auto& entry : desc.GetExplicitEntryList().GetEntries()) {
        TPath path = TPath::Resolve(entry.GetPath(), this);
        if (path.IsResolved() && path->IsTable()) {
            TableInBackupCollections.erase(path.Base()->PathId);
        }
    }
}

void TSchemeShard::PersistSecret(NIceDb::TNiceDb& db, TPathId pathId, const TSecretInfo& secretInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(secretInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::Secrets>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Secrets::AlterVersion>(secretInfo.AlterVersion),
        NIceDb::TUpdate<Schema::Secrets::Description>(serializedDescription));
}

void TSchemeShard::PersistSecret(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Secrets.contains(pathId));
    TSecretInfo::TPtr secretInfo = Secrets.at(pathId);

    Y_ABORT_UNLESS(elem->IsSecret());

    Y_ABORT_UNLESS(secretInfo);

    PersistSecret(db, pathId, *secretInfo);
}

void TSchemeShard::PersistSecretRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (!Secrets.contains(pathId)) {
        return;
    }

    auto secretInfo = Secrets.at(pathId);
    if (secretInfo->AlterData) {
        secretInfo->AlterData = nullptr;
        PersistSecretAlterRemove(db, pathId);
    }

    Secrets.erase(pathId);
    DecrementPathDbRefCount(pathId);
    db.Table<Schema::Secrets>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSecretAlter(NIceDb::TNiceDb& db, TPathId pathId, const TSecretInfo& secretInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(secretInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::SecretsAlterData>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SecretsAlterData::AlterVersion>(secretInfo.AlterVersion),
        NIceDb::TUpdate<Schema::SecretsAlterData::Description>(serializedDescription));
}

void TSchemeShard::PersistSecretAlter(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Secrets.contains(pathId));
    TSecretInfo::TPtr secretInfo = Secrets.at(pathId);

    Y_ABORT_UNLESS(elem->IsSecret());

    TSecretInfo::TPtr alterData = secretInfo->AlterData;
    Y_ABORT_UNLESS(alterData);

    PersistSecretAlter(db, pathId, *alterData);
}

void TSchemeShard::PersistSecretAlterRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SecretsAlterData>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistStreamingQuery(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto path = PathsById.find(pathId);
    Y_ABORT_UNLESS(path != PathsById.end());
    Y_ABORT_UNLESS(path->second && path->second->IsStreamingQuery());

    const auto streamingQueryIt = StreamingQueries.find(pathId);
    Y_ABORT_UNLESS(streamingQueryIt != StreamingQueries.end());
    const auto streamingQuery = streamingQueryIt->second;
    Y_ABORT_UNLESS(streamingQuery);

    db.Table<Schema::StreamingQueryState>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::StreamingQueryState::AlterVersion>{streamingQuery->AlterVersion},
        NIceDb::TUpdate<Schema::StreamingQueryState::Properties>{streamingQuery->Properties.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveStreamingQuery(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (const auto it = StreamingQueries.find(pathId); it != StreamingQueries.end()) {
        StreamingQueries.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::StreamingQueryState>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistTestShardSet(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto path = PathsById.find(pathId);
    Y_ABORT_UNLESS(path != PathsById.end());
    Y_ABORT_UNLESS(path->second && path->second->IsTestShardSet());

    const auto testShardSetIt = TestShardSets.find(pathId);
    Y_ABORT_UNLESS(testShardSetIt != TestShardSets.end());
    const auto testShardSet = testShardSetIt->second;
    Y_ABORT_UNLESS(testShardSet);

    NKikimrSchemeOp::TTestShardSetDescription description;
    for (const auto& [shardIdx, tabletId] : testShardSet->TestShards) {
        auto* shardDesc = description.AddShards();
        shardDesc->SetShardIdx(ui64(shardIdx.GetLocalId()));
        shardDesc->SetTabletId(ui64(tabletId));
    }
    TString serializedTestShards;
    Y_ABORT_UNLESS(description.SerializeToString(&serializedTestShards));

    TString serializedCmdInitialize;
    Y_ABORT_UNLESS(testShardSet->CmdInitialize.SerializeToString(&serializedCmdInitialize));

    db.Table<Schema::TestShardSet>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::TestShardSet::AlterVersion>(testShardSet->AlterVersion),
        NIceDb::TUpdate<Schema::TestShardSet::TestShards>(serializedTestShards),
        NIceDb::TUpdate<Schema::TestShardSet::CmdInitialize>(serializedCmdInitialize)
    );
}

void TSchemeShard::PersistRemoveTestShardSet(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (const auto it = TestShardSets.find(pathId); it != TestShardSets.end()) {
        TestShardSets.erase(it);
        DecrementPathDbRefCount(pathId);
    }
    db.Table<Schema::TestShardSet>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = RtmrVolumes.find(pathId);
    if (it != RtmrVolumes.end()) {
        TRtmrVolumeInfo::TPtr rtmrVol = it->second;

        for (const auto& partition : rtmrVol->Partitions) {
            db.Table<Schema::RTMRPartitions>().Key(pathId.LocalPathId, partition.second->ShardIdx.GetLocalId()).Delete();
        }

        RtmrVolumes.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::RtmrVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr solomonVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SolomonVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SolomonVolumes::Version>(solomonVol->Version));

    db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    for (const auto& part: solomonVol->Partitions) {
        db.Table<Schema::SolomonPartitions>().Key(pathId.LocalPathId, part.first.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::SolomonPartitions::PartitionId>(part.second->PartitionId));

        db.Table<Schema::AlterSolomonPartitions>()
            .Key(
                pathId.OwnerId, pathId.LocalPathId,
                part.first.GetOwnerId(), part.first.GetLocalId())
            .Delete();
    }
}

void TSchemeShard::PersistRemoveSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = SolomonVolumes.find(pathId);
    if (it != SolomonVolumes.end()) {
        TSolomonVolumeInfo::TPtr solomonVol = it->second;

        if (solomonVol->AlterData) {
            for (const auto& part : solomonVol->AlterData->Partitions) {
                db.Table<Schema::AlterSolomonPartitions>()
                    .Key(
                        pathId.OwnerId, pathId.LocalPathId,
                        part.first.GetOwnerId(), part.first.GetLocalId())
                    .Delete();
            }
            db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
        }

        for (const auto& part : solomonVol->Partitions) {
            db.Table<Schema::SolomonPartitions>().Key(pathId.LocalPathId, part.first.GetLocalId()).Delete();
        }

        SolomonVolumes.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::SolomonVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistAlterSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr solomonVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(solomonVol->AlterData);


    db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::AlterSolomonVolumes::Version>(solomonVol->AlterData->Version));

    for (const auto& part: solomonVol->AlterData->Partitions) {
        db.Table<Schema::AlterSolomonPartitions>()
            .Key(
                pathId.OwnerId, pathId.LocalPathId,
                part.first.GetOwnerId(), part.first.GetLocalId())
            .Update(
                NIceDb::TUpdate<Schema::AlterSolomonPartitions::PartitionId>(part.second->PartitionId));
    }
}

void TSchemeShard::PersistAddTxDependency(NIceDb::TNiceDb& db, const TTxId parentOpId, const TTxId opId) {
    db.Table<Schema::TxDependencies>().Key(parentOpId, opId).Update();
}

void TSchemeShard::PersistRemoveTxDependency(NIceDb::TNiceDb& db, TTxId opId, TTxId dependentOpId) {
    db.Table<Schema::TxDependencies>().Key(opId, dependentOpId).Delete();
}

void TSchemeShard::PersistUpdateTxShard(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx, ui32 operation) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::TxShardsV2>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::TxShardsV2::Operation>(operation));
    } else {
        db.Table<Schema::MigratedTxShards>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedTxShards::Operation>(operation));
    }
}

void TSchemeShard::PersistRemoveTxShard(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx) {
    if (FirstSubTxId == opId.GetSubTxId()) {
        db.Table<Schema::TxShards>().Key(opId.GetTxId(), shardIdx.GetLocalId()).Delete();
    }

    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::TxShardsV2>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedTxShards>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistUpdateNextPathId(NIceDb::TNiceDb& db) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_NextPathId).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>(ToString(NextLocalPathId)));
}

void TSchemeShard::PersistUpdateNextShardIdx(NIceDb::TNiceDb& db) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_NextShardIdx).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>(ToString(NextLocalShardIdx)));
}

void TSchemeShard::PersistParentDomain(NIceDb::TNiceDb& db, TPathId parentDomain) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainSchemeShard).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(parentDomain.OwnerId)));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainPathId).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(parentDomain.LocalPathId)));
}

void TSchemeShard::PersistParentDomainEffectiveACL(NIceDb::TNiceDb& db, const TString& owner, const TString& effectiveACL, ui64 effectiveACLVersion) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainOwner).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(owner));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainEffectiveACL).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(effectiveACL));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainEffectiveACLVersion).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(effectiveACLVersion)));
}

void TSchemeShard::PersistShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, TPathId pathId, TTxId txId, TTabletTypes::EType type) {
    if (IsLocalId(shardIdx)) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::TabletId>(tabletId),
                NIceDb::TUpdate<Schema::Shards::OwnerPathId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::Shards::PathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::Shards::LastTxId>(txId),
                NIceDb::TUpdate<Schema::Shards::TabletType>(type));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::TabletId>(tabletId),
            NIceDb::TUpdate<Schema::MigratedShards::OwnerPathId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::MigratedShards::LocalPathId>(pathId.LocalPathId),
            NIceDb::TUpdate<Schema::MigratedShards::LastTxId>(txId),
            NIceDb::TUpdate<Schema::MigratedShards::TabletType>(type));
    }
}

void TSchemeShard::PersistAdoptedShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, ui64 prevOwner, TLocalShardIdx prevShardIdx) {
    Y_ABORT_UNLESS(IsLocalId(shardIdx));
    db.Table<Schema::AdoptedShards>().Key(shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::AdoptedShards::PrevOwner>(prevOwner),
            NIceDb::TUpdate<Schema::AdoptedShards::PrevShardIdx>(prevShardIdx),
            NIceDb::TUpdate<Schema::AdoptedShards::TabletId>(tabletId));
}

void TSchemeShard::PersistShardPathId(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId) {
    if (IsLocalId(shardIdx)) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::OwnerPathId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::Shards::PathId>(pathId.LocalPathId));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::OwnerPathId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::MigratedShards::LocalPathId>(pathId.LocalPathId));
    }
}

void TSchemeShard::PersistAddSharedShard(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId) {
    db.Table<Schema::SharedShards>().Key(shardIdx.GetLocalId(), pathId.OwnerId, pathId.LocalPathId).Update();
}

void TSchemeShard::PersistRemoveSharedShard(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId) {
    db.Table<Schema::SharedShards>().Key(shardIdx.GetLocalId(), pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSharedShardTx(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId, TTxId txId) {
    db.Table<Schema::SharedShards>().Key(shardIdx.GetLocalId(), pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SharedShards::LastTxId>(ui64(txId)));
}

void TSchemeShard::PersistDeleteAdopted(NIceDb::TNiceDb& db, TShardIdx shardIdx) {
    Y_ABORT_UNLESS(IsLocalId(shardIdx));
    db.Table<Schema::AdoptedShards>().Key(shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistShardTx(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTxId txId) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::LastTxId>(txId));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::LastTxId>(txId));
    }
}

void TSchemeShard::PersistShardsToDelete(NIceDb::TNiceDb& db, const THashSet<TShardIdx>& shardsIdxs) {
    for (auto& shardIdx : shardsIdxs) {
        if (shardIdx.GetOwnerId() == TabletID()) {
            db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Update();
        } else {
            db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update();
        }
    }
}

void TSchemeShard::PersistSystemShardsToDelete(NIceDb::TNiceDb& db, const THashSet<TShardIdx>& shardsIdxs) {
    for (auto& shardIdx : shardsIdxs) {
        Y_ABORT_UNLESS(IsLocalId(shardIdx));
        db.Table<Schema::SystemShardsToDelete>().Key(shardIdx.GetLocalId()).Update();
    }
}

void TSchemeShard::PersistShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx, const TChannelsBindings& bindedChannels) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
        db.Table<Schema::SystemShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Delete();
        for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
            db.Table<Schema::ChannelsBinding>().Key(shardIdx.GetLocalId(), channelId).Delete();
        }
        db.Table<Schema::TableShardPartitionConfigs>().Key(shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
        db.Table<Schema::MigratedChannelsBinding>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId(), channelId).Delete();
    }
    db.Table<Schema::MigratedTableShardPartitionConfigs>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistUnknownShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
        db.Table<Schema::SystemShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistTxShardStatus(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx, const TTxState::TShardStatus& status) {
    db.Table<Schema::TxShardStatus>()
        .Key(opId.GetTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId())
        .Update(
            NIceDb::TUpdate<Schema::TxShardStatus::Success>(status.Success),
            NIceDb::TUpdate<Schema::TxShardStatus::Error>(status.Error),
            NIceDb::TUpdate<Schema::TxShardStatus::BytesProcessed>(status.BytesProcessed),
            NIceDb::TUpdate<Schema::TxShardStatus::RowsProcessed>(status.RowsProcessed)
        );
}

NKikimrSchemeOp::TChangefeedUnderlyingTopics ConvertChangefeedUnderlyingTopics(
    const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TPathDescription>& changefeedUnderlyingTopics
) {
    NKikimrSchemeOp::TChangefeedUnderlyingTopics result;
    for (const auto& x : changefeedUnderlyingTopics) {
        *result.AddChangefeedUnderlyingTopics() = x;
    }
    return result;
}

void TSchemeShard::PersistBackupSettings(
        NIceDb::TNiceDb& db,
        TPathId pathId,
        const NKikimrSchemeOp::TBackupTask& settings)
{
#define PERSIST_BACKUP_SETTINGS(Kind) \
    if (settings.Has##Kind()) { \
        if (IsLocalId(pathId)) { \
            db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Update( \
                NIceDb::TUpdate<Schema::BackupSettings::TableName>(settings.GetTableName()), \
                NIceDb::TUpdate<Schema::BackupSettings::Kind>(settings.Get##Kind().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::ScanSettings>(settings.GetScanSettings().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::NeedToBill>(settings.GetNeedToBill()), \
                NIceDb::TUpdate<Schema::BackupSettings::TableDescription>(settings.GetTable().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::ChangefeedUnderlyingTopics>(ConvertChangefeedUnderlyingTopics(settings.GetChangefeedUnderlyingTopics()).SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::NumberOfRetries>(settings.GetNumberOfRetries()), \
                NIceDb::TUpdate<Schema::BackupSettings::EnableChecksums>(settings.GetEnableChecksums()), \
                NIceDb::TUpdate<Schema::BackupSettings::EnablePermissions>(settings.GetEnablePermissions())); \
        } else { \
            db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Update( \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::TableName>(settings.GetTableName()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::Kind>(settings.Get##Kind().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::ScanSettings>(settings.GetScanSettings().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::NeedToBill>(settings.GetNeedToBill()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::TableDescription>(settings.GetTable().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::ChangefeedUnderlyingTopics>(ConvertChangefeedUnderlyingTopics(settings.GetChangefeedUnderlyingTopics()).SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::NumberOfRetries>(settings.GetNumberOfRetries()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::EnableChecksums>(settings.GetEnableChecksums()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::EnablePermissions>(settings.GetEnablePermissions())); \
        } \
    }

    PERSIST_BACKUP_SETTINGS(YTSettings)
    PERSIST_BACKUP_SETTINGS(S3Settings)
    PERSIST_BACKUP_SETTINGS(FSSettings)

#undef PERSIST_BACKUP_SETTINGS
}

void TSchemeShard::PersistCompletedBackupRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& info, TTableInfo::TBackupRestoreResult::EKind kind) {
    TPathId pathId = txState.TargetPathId;

    if (IsLocalId(pathId)) {
        db.Table<Schema::CompletedBackups>().Key(pathId.LocalPathId, txId, info.CompletionDateTime).Update(
            NIceDb::TUpdate<Schema::CompletedBackups::TotalShardCount>(info.TotalShardCount),
            NIceDb::TUpdate<Schema::CompletedBackups::SuccessShardCount>(info.SuccessShardCount),
            NIceDb::TUpdate<Schema::CompletedBackups::StartTime>(info.StartDateTime),
            NIceDb::TUpdate<Schema::CompletedBackups::DataTotalSize>(txState.DataTotalSize),
            NIceDb::TUpdate<Schema::CompletedBackups::Kind>(static_cast<ui8>(kind)));
    } else {
        db.Table<Schema::MigratedCompletedBackups>()
            .Key(pathId.OwnerId, pathId.LocalPathId,
                 txId,
                 info.CompletionDateTime)
            .Update(NIceDb::TUpdate<Schema::MigratedCompletedBackups::TotalShardCount>(info.TotalShardCount),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::SuccessShardCount>(info.SuccessShardCount),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::StartTime>(info.StartDateTime),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::DataTotalSize>(txState.DataTotalSize),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::Kind>(static_cast<ui8>(kind)));
    }
}

void TSchemeShard::PersistCompletedBackup(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& backupInfo) {
    PersistCompletedBackupRestore(db, txId, txState, backupInfo, TTableInfo::TBackupRestoreResult::EKind::Backup);
}

void TSchemeShard::PersistCompletedRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& restoreInfo) {
    PersistCompletedBackupRestore(db, txId, txState, restoreInfo, TTableInfo::TBackupRestoreResult::EKind::Restore);
}

void TSchemeShard::PersistBackupDone(NIceDb::TNiceDb& db, TPathId pathId) {
    if (IsLocalId(pathId)) {
        db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Delete();
    }

    db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId, TShardIdx shardIdx, ui64 version)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStorePartitions>().Key(pathId.LocalPathId, partitionId).Update(
        NIceDb::TUpdate<Schema::BlockStorePartitions::ShardIdx>(shardIdx.GetLocalId()),
        NIceDb::TUpdate<Schema::BlockStorePartitions::AlterVersion>(version));
}

void TSchemeShard::PersistBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString volumeConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD volume->VolumeConfig.SerializeToString(&volumeConfig);
    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumes::VolumeConfig>(volumeConfig),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::AlterVersion>(volume->AlterVersion),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::MountToken>(volume->MountToken));
}

void TSchemeShard::PersistBlockStoreVolumeMountToken(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumes::MountToken>(volume->MountToken),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::TokenVersion>(volume->TokenVersion));
}

void TSchemeShard::PersistAddBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString volumeConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD volume->VolumeConfig.SerializeToString(&volumeConfig);
    db.Table<Schema::BlockStoreVolumeAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::VolumeConfig>(volumeConfig),
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::AlterVersion>(volume->AlterVersion),
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::PartitionCount>(volume->DefaultPartitionCount));
}

void TSchemeShard::PersistRemoveBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStoreVolumeAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStorePartitions>().Key(pathId.LocalPathId, partitionId).Delete();
}

void TSchemeShard::PersistRemoveBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (BlockStoreVolumes.contains(pathId)) {
        auto volume = BlockStoreVolumes.at(pathId);

        if (volume->AlterData) {
            PersistRemoveBlockStoreVolumeAlter(db, pathId);
        }

        for (auto& shard : volume->Shards) {
            const auto& part = shard.second;
            PersistRemoveBlockStorePartition(db, pathId, part->PartitionId);
        }

        BlockStoreVolumes.erase(pathId);

        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr fs)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD fs->Config.SerializeToString(&config);

    db.Table<Schema::FileStoreInfos>()
        .Key(pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::FileStoreInfos::Config>(config),
            NIceDb::TUpdate<Schema::FileStoreInfos::Version>(fs->Version));
}

void TSchemeShard::PersistAddFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr fs)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD fs->AlterConfig->SerializeToString(&config);

    db.Table<Schema::FileStoreAlters>()
        .Key(pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::FileStoreAlters::Config>(config),
            NIceDb::TUpdate<Schema::FileStoreAlters::Version>(fs->AlterVersion));
}

void TSchemeShard::PersistRemoveFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::FileStoreAlters>()
        .Key(pathId.LocalPathId)
        .Delete();
}

void TSchemeShard::PersistRemoveFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (FileStoreInfos.contains(pathId)) {
        auto fs = FileStoreInfos.at(pathId);

        if (fs->AlterConfig) {
            PersistRemoveFileStoreAlter(db, pathId);
        }

        FileStoreInfos.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::FileStoreInfos>()
        .Key(pathId.LocalPathId)
        .Delete();
}

void TSchemeShard::PersistOlapStore(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serialized;
    TString serializedSharding;
    Y_ABORT_UNLESS(storeInfo.GetDescription().SerializeToString(&serialized));
    Y_ABORT_UNLESS(storeInfo.Sharding.SerializeToString(&serializedSharding));

    if (isAlter) {
        db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::OlapStoresAlters::AlterVersion>(storeInfo.GetAlterVersion()),
            NIceDb::TUpdate<Schema::OlapStoresAlters::Description>(serialized),
            NIceDb::TUpdate<Schema::OlapStoresAlters::Sharding>(serializedSharding));
        if (storeInfo.AlterBody) {
            TString serializedAlterBody;
            Y_ABORT_UNLESS(storeInfo.AlterBody->SerializeToString(&serializedAlterBody));
            db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::OlapStoresAlters::AlterBody>(serializedAlterBody));
        }
    } else {
        db.Table<Schema::OlapStores>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::OlapStores::AlterVersion>(storeInfo.GetAlterVersion()),
            NIceDb::TUpdate<Schema::OlapStores::Description>(serialized),
            NIceDb::TUpdate<Schema::OlapStores::Sharding>(serializedSharding));
    }
}

void TSchemeShard::PersistOlapStoreRemove(NIceDb::TNiceDb& db, TPathId pathId, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (isAlter) {
        db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Delete();
        return;
    }

    if (!OlapStores.contains(pathId)) {
        return;
    }

    auto storeInfo = OlapStores.at(pathId);
    if (storeInfo->AlterData) {
        PersistOlapStoreAlterRemove(db, pathId);
    }

    db.Table<Schema::OlapStores>().Key(pathId.LocalPathId).Delete();
    OlapStores.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistOlapStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo)
{
    PersistOlapStore(db, pathId, storeInfo, true);
}

void TSchemeShard::PersistOlapStoreAlterRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    PersistOlapStoreRemove(db, pathId, true);
}

void TSchemeShard::PersistColumnTable(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serialized;
    TString serializedSharding;
    auto tableInfoCopy = tableInfo;
    if (tableInfo.IsStandalone()) {
        tableInfoCopy.Description.MutableSchema()->SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
    }

    // Keep multi-column statistics out of the Description blob
    TString serializedStatistics;
    {
        NKikimrSchemeOp::TColumnTableDescription statisticsHolder;
        statisticsHolder.MutableMultiColumnStatistics()->CopyFrom(tableInfoCopy.Description.GetMultiColumnStatistics());
        Y_ABORT_UNLESS(statisticsHolder.SerializeToString(&serializedStatistics));
        tableInfoCopy.Description.ClearMultiColumnStatistics();
    }

    Y_ABORT_UNLESS(tableInfoCopy.Description.SerializeToString(&serialized));
    Y_ABORT_UNLESS(tableInfoCopy.Description.GetSharding().SerializeToString(&serializedSharding));

    if (isAlter) {
        db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ColumnTablesAlters::AlterVersion>(tableInfo.AlterVersion),
            NIceDb::TUpdate<Schema::ColumnTablesAlters::Description>(serialized),
            NIceDb::TUpdate<Schema::ColumnTablesAlters::Sharding>(serializedSharding),
            NIceDb::TUpdate<Schema::ColumnTablesAlters::MultiColumnStatistics>(serializedStatistics));
        if (tableInfo.AlterBody) {
            TString serializedAlterBody;
            Y_ABORT_UNLESS(tableInfo.AlterBody->SerializeToString(&serializedAlterBody));
            db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTablesAlters::AlterBody>(serializedAlterBody));
        }
        if (tableInfo.StandaloneSharding) {
            TString serializedOwnedShards;
            Y_ABORT_UNLESS(tableInfo.StandaloneSharding->SerializeToString(&serializedOwnedShards));
            db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTablesAlters::StandaloneSharding>(serializedOwnedShards));
        }
    } else {
        db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ColumnTables::AlterVersion>(tableInfo.AlterVersion),
            NIceDb::TUpdate<Schema::ColumnTables::Description>(serialized),
            NIceDb::TUpdate<Schema::ColumnTables::Sharding>(serializedSharding),
            NIceDb::TUpdate<Schema::ColumnTables::IsReadOnly>(tableInfo.IsReadOnly),
            NIceDb::TUpdate<Schema::ColumnTables::MultiColumnStatistics>(serializedStatistics));
        if (tableInfo.StandaloneSharding) {
            TString serializedOwnedShards;
            Y_ABORT_UNLESS(tableInfo.StandaloneSharding->SerializeToString(&serializedOwnedShards));
            db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTables::StandaloneSharding>(serializedOwnedShards));
        }
    }
}

void TSchemeShard::UpdateDiskSpaceUsage(NIceDb::TNiceDb& db, TPathId pathId, const TPartitionStats& newPartitionStats, const TPartitionStats& oldPartitionStats, const TActorContext &ctx) {
    auto subDomainId = ResolvePathIdForDomain(pathId);
    auto subDomainInfo = ResolveDomainInfo(pathId);
    subDomainInfo->AggrDiskSpaceUsage(this, newPartitionStats, oldPartitionStats);
    if (subDomainInfo->CheckDiskSpaceQuotas(this)) {
        PersistSubDomainState(db, subDomainId, *subDomainInfo);
        // Publish is done in a separate transaction, so we may call this directly
        TDeque<TPathId> toPublish;
        toPublish.push_back(subDomainId);
        PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
    }
}

void TSchemeShard::PersistColumnTableRemove(NIceDb::TNiceDb& db, TPathId pathId, const TActorContext &ctx, bool skipStatsUpdate)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    auto tablePtr = ColumnTables.at(pathId);
    if (!tablePtr) {
        return;
    }
    auto& tableInfo = *tablePtr;

    if (tableInfo.AlterData) {
        PersistColumnTableAlterRemove(db, pathId);
    }

    // Unlink table from olap store
    if (!tableInfo.IsStandalone() && tableInfo.GetOlapStorePathIdVerified()) {
        Y_ABORT_UNLESS(OlapStores.contains(tableInfo.GetOlapStorePathIdVerified()));
        auto storeInfo = OlapStores.at(tableInfo.GetOlapStorePathIdVerified());
        storeInfo->ColumnTablesUnderOperation.erase(pathId);
        storeInfo->ColumnTables.erase(pathId);
    }

    if (!skipStatsUpdate) {
        UpdateDiskSpaceUsage(db, pathId, TPartitionStats(), tableInfo.GetStats().Aggregated, ctx);
    }

    ClearBackupRestoreHistory(db, pathId, tableInfo.BackupHistory);
    ClearBackupRestoreHistory(db, pathId, tableInfo.RestoreHistory);

    if (IsLocalId(pathId)) {
        db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    db.Table<Schema::RestoreTasks>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Delete();
    ColumnTables.Drop(pathId);
    DecrementPathDbRefCount(pathId);

    auto ev = MakeHolder<NSysView::TEvSysView::TEvRemoveTable>(GetDomainKey(pathId), pathId);
    Send(SysPartitionStatsCollector, ev.Release());
}

void TSchemeShard::PersistColumnTableAlter(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo) {
    PersistColumnTable(db, pathId, tableInfo, true);
}

void TSchemeShard::PersistColumnTableAlterRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSequence(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    TString serializedSharding;
    Y_ABORT_UNLESS(sequenceInfo.Description.SerializeToString(&serializedDescription));
    Y_ABORT_UNLESS(sequenceInfo.Sharding.SerializeToString(&serializedSharding));

    db.Table<Schema::Sequences>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Sequences::AlterVersion>(sequenceInfo.AlterVersion),
        NIceDb::TUpdate<Schema::Sequences::Description>(serializedDescription),
        NIceDb::TUpdate<Schema::Sequences::Sharding>(serializedSharding));
}

void TSchemeShard::PersistSequence(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Sequences.contains(pathId));
    TSequenceInfo::TPtr sequenceInfo = Sequences.at(pathId);

    Y_ABORT_UNLESS(elem->IsSequence());

    Y_ABORT_UNLESS(sequenceInfo);

    PersistSequence(db, pathId, *sequenceInfo);
}

void TSchemeShard::PersistSequenceRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (!Sequences.contains(pathId)) {
        return;
    }

    auto sequenceInfo = Sequences.at(pathId);
    if (sequenceInfo->AlterData) {
        PersistSequenceAlterRemove(db, pathId);
        sequenceInfo->AlterData = nullptr;
    }

    db.Table<Schema::Sequences>().Key(pathId.LocalPathId).Delete();
    Sequences.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    TString serializedSharding;
    Y_ABORT_UNLESS(sequenceInfo.Description.SerializeToString(&serializedDescription));
    Y_ABORT_UNLESS(sequenceInfo.Sharding.SerializeToString(&serializedSharding));

    db.Table<Schema::SequencesAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SequencesAlters::AlterVersion>(sequenceInfo.AlterVersion),
        NIceDb::TUpdate<Schema::SequencesAlters::Description>(serializedDescription),
        NIceDb::TUpdate<Schema::SequencesAlters::Sharding>(serializedSharding));
}


} // namespace NSchemeShard
} // namespace NKikimr
