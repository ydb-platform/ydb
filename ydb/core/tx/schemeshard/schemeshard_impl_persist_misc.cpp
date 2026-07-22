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
void TSchemeShard::PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Sequences.contains(pathId));
    TSequenceInfo::TPtr sequenceInfo = Sequences.at(pathId);

    Y_ABORT_UNLESS(elem->IsSequence());

    TSequenceInfo::TPtr alterData = sequenceInfo->AlterData;
    Y_ABORT_UNLESS(alterData);

    PersistSequenceAlter(db, pathId, *alterData);
}

void TSchemeShard::PersistSequenceAlterRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SequencesAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistReplication(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(replicationInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::Replications>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Replications::AlterVersion>(replicationInfo.AlterVersion),
        NIceDb::TUpdate<Schema::Replications::Description>(serializedDescription));
}

void TSchemeShard::PersistReplicationRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (!Replications.contains(pathId)) {
        return;
    }

    auto replicationInfo = Replications.at(pathId);
    if (replicationInfo->AlterData) {
        replicationInfo->AlterData = nullptr;
        PersistReplicationAlterRemove(db, pathId);
    }

    Replications.erase(pathId);
    DecrementPathDbRefCount(pathId);
    db.Table<Schema::Replications>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistReplicationAlter(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(replicationInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::ReplicationsAlterData>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ReplicationsAlterData::AlterVersion>(replicationInfo.AlterVersion),
        NIceDb::TUpdate<Schema::ReplicationsAlterData::Description>(serializedDescription));
}

void TSchemeShard::PersistReplicationAlterRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ReplicationsAlterData>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistBlobDepot(NIceDb::TNiceDb& db, TPathId pathId, const TBlobDepotInfo& blobDepotInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString description;
    const bool success = blobDepotInfo.Description.SerializeToString(&description);
    Y_ABORT_UNLESS(success);

    using T = Schema::BlobDepots;
    db.Table<T>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<T::AlterVersion>(blobDepotInfo.AlterVersion),
        NIceDb::TUpdate<T::Description>(description)
    );
}

void TSchemeShard::PersistKesusInfo(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr kesus)
{
    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD kesus->Config.SerializeToString(&config);

    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusInfos::Config>(config),
            NIceDb::TUpdate<Schema::KesusInfos::Version>(kesus->Version));
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Config>(config),
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Version>(kesus->Version));
    }
}

void TSchemeShard::PersistKesusVersion(NIceDb::TNiceDb &db, TPathId pathId, const TKesusInfo::TPtr kesus) {
    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusInfos::Version>(kesus->Version));
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Version>(kesus->Version));
    }
}

void TSchemeShard::PersistAddKesusAlter(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr kesus)
{
    Y_ABORT_UNLESS(kesus->AlterConfig);
    Y_ABORT_UNLESS(kesus->AlterVersion);
    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD kesus->AlterConfig->SerializeToString(&config);

    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusAlters::Config>(config),
            NIceDb::TUpdate<Schema::KesusAlters::Version>(kesus->AlterVersion));
    } else {
        db.Table<Schema::MigratedKesusAlters>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusAlters::Config>(config),
            NIceDb::TUpdate<Schema::MigratedKesusAlters::Version>(kesus->AlterVersion));
    }
}

void TSchemeShard::PersistRemoveKesusAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusAlters>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedKesusAlters>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveKesusInfo(NIceDb::TNiceDb& db, TPathId pathId)
{
    if (KesusInfos.contains(pathId)) {
        auto kesus = KesusInfos.at(pathId);

        if (kesus->AlterConfig) {
            PersistRemoveKesusAlter(db, pathId);
        }

        KesusInfos.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    if (IsLocalId(pathId)) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Delete();
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
}

void TSchemeShard::PersistRevertedMigration(NIceDb::TNiceDb& db, TPathId pathId, TTabletId abandonedSchemeShardId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::RevertedMigrations>().Key(pathId.LocalPathId, abandonedSchemeShardId).Update();
}

void TSchemeShard::ClearBackupRestoreHistory(NIceDb::TNiceDb& db, TPathId pathId, const TMap<TTxId, TTableInfo::TBackupRestoreResult>& history) {
    for (const auto& [txId, result] : history) {
        for (const auto& [shard, _] : result.ShardStatuses) {
            if (IsLocalId(shard)) {
                db.Table<Schema::ShardBackupStatus>().Key(txId, shard.GetLocalId()).Delete();
            }
            db.Table<Schema::MigratedShardBackupStatus>().Key(txId, shard.GetOwnerId(), shard.GetLocalId()).Delete();
            db.Table<Schema::TxShardStatus>().Key(txId, shard.GetOwnerId(), shard.GetLocalId()).Delete();
        }

        if (IsLocalId(pathId)) {
            db.Table<Schema::CompletedBackups>().Key(pathId.LocalPathId, txId, result.CompletionDateTime).Delete();
        }
        db.Table<Schema::MigratedCompletedBackups>().Key(pathId.OwnerId, pathId.LocalPathId, txId, result.CompletionDateTime).Delete();
    }
}

void TSchemeShard::PersistRemoveTable(NIceDb::TNiceDb& db, TPathId pathId, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    const TPathElement::TPtr path = PathsById.at(pathId);

    if (!Tables.contains(pathId)) {
        return;
    }
    const TTableInfo::TPtr tableInfo = Tables.at(pathId);

    ClearBackupRestoreHistory(db, pathId, tableInfo->BackupHistory);
    ClearBackupRestoreHistory(db, pathId, tableInfo->RestoreHistory);

    if (IsLocalId(pathId)) {
        db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    db.Table<Schema::RestoreTasks>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    if (tableInfo->AlterData) {
        auto& alterData = tableInfo->AlterData;
        for (auto& cItem: alterData->Columns) {
            if (pathId.OwnerId == TabletID()) {
                db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, cItem.first).Delete();
            }
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, cItem.first).Delete();
        }
    }

    for (auto& cItem: tableInfo->Columns) {
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::Columns>().Key(pathId.LocalPathId, cItem.first).Delete();
        }
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, cItem.first).Delete();
    }

    for (ui32 pNo = 0; pNo < tableInfo->GetPartitions().size(); ++pNo) {
        const auto* shardInfo = tableInfo->GetPartitions().at(pNo);

        if (tableInfo->PartitionsInShardIdxFormat) {
            db.Table<Schema::TablePartitionsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId,
                     shardInfo->ShardIdx.GetOwnerId(), shardInfo->ShardIdx.GetLocalId())
                .Delete();
            db.Table<Schema::TablePartitionStatsByShardIdx>()
                .Key(pathId.OwnerId, pathId.LocalPathId,
                     shardInfo->ShardIdx.GetOwnerId(), shardInfo->ShardIdx.GetLocalId())
                .Delete();
        } else {
            if (pathId.OwnerId == TabletID()) {
                db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pNo).Delete();
            }
            db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pNo).Delete();
        }
        db.Table<Schema::TablePartitionStats>().Key(pathId.OwnerId, pathId.LocalPathId, pNo).Delete();

        if (auto& lag = shardInfo->LastCondEraseLag) {
            TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
            lag.Clear();
        }
    }

    for (const auto& [_, childPathId]: path->GetChildren()) {
        Y_ABORT_UNLESS(PathsById.contains(childPathId));
        auto childPath = PathsById.at(childPathId);

        if (childPath->IsTableIndex()) {
            PersistRemoveTableIndex(db, childPathId);
        }
    }

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    if (tableInfo->IsTTLEnabled()) {
        TTLEnabledTables.erase(pathId);
        TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Sub(1);
    }

    if (tableInfo->PartitionsInShardIdxFormat) {
        TabletCounters->Simple()[COUNTER_FORMAT_SHARDIDX_TABLE_COUNT].Sub(1);
    } else {
        TabletCounters->Simple()[COUNTER_FORMAT_POSITION_TABLE_COUNT].Sub(1);
    }

    if (TablesWithSnapshots.contains(pathId)) {
        const TTxId snapshotId = TablesWithSnapshots.at(pathId);
        PersistDropSnapshot(db, snapshotId, pathId);

        TablesWithSnapshots.erase(pathId);
        SnapshotTables.at(snapshotId).erase(pathId);
        if (SnapshotTables.at(snapshotId).empty()) {
            SnapshotTables.erase(snapshotId);
        }
        SnapshotsStepIds.erase(snapshotId);
    }

    if (!tableInfo->IsBackup && !tableInfo->IsShardsStatsDetached()) {
        UpdateDiskSpaceUsage(db, pathId, TPartitionStats(), tableInfo->GetStats().Aggregated, ctx);
    }

    // sanity check: by this time compaction queue and metrics must be updated already
    for (const auto& shardIdx : tableInfo->GetPartitionStore() | std::views::keys) {
        OnShardRemoved(shardIdx);
    }

    Tables.erase(pathId);
    DecrementPathDbRefCount(pathId, "remove table");

    auto ev = MakeHolder<NSysView::TEvSysView::TEvRemoveTable>(GetDomainKey(pathId), pathId);
    Send(SysPartitionStatsCollector, ev.Release());
}

void TSchemeShard::PersistRemoveTableIndex(NIceDb::TNiceDb &db, TPathId pathId)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    const TPathElement::TPtr path = PathsById.at(pathId);

    if (!Indexes.contains(pathId)) {
        return;
    }

    const TTableIndexInfo::TPtr index = Indexes.at(pathId);
    for (ui32 kNo = 0; kNo < index->IndexKeys.size(); ++kNo) {
        if (IsLocalId(pathId)) {
            db.Table<Schema::TableIndexKeys>().Key(pathId.LocalPathId, kNo).Delete();
        }
        db.Table<Schema::MigratedTableIndexKeys>().Key(pathId.OwnerId, pathId.LocalPathId, kNo).Delete();
    }

    for (ui32 dNo = 0; dNo < index->IndexDataColumns.size(); ++dNo) {
        db.Table<Schema::TableIndexDataColumns>().Key(pathId.OwnerId, pathId.LocalPathId, dNo).Delete();
    }

    if (index->AlterData) {
        auto alterData = index->AlterData;
        for (ui32 kNo = 0; kNo < alterData->IndexKeys.size(); ++kNo) {
            db.Table<Schema::TableIndexKeysAlterData>().Key(pathId.LocalPathId, kNo).Delete();
        }

        for (ui32 dNo = 0; dNo < alterData->IndexDataColumns.size(); ++dNo) {
            db.Table<Schema::TableIndexDataColumnsAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, dNo).Delete();
        }

        db.Table<Schema::TableIndexAlterData>().Key(pathId.LocalPathId).Delete();
    }

    if (IsLocalId(pathId)) {
        db.Table<Schema::TableIndex>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedTableIndex>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    Indexes.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistAddTableShardPartitionConfig(NIceDb::TNiceDb& db, TShardIdx shardIdx, const NKikimrSchemeOp::TPartitionConfig& config)
{
    TString data;
    Y_PROTOBUF_SUPPRESS_NODISCARD config.SerializeToString(&data);

    if (IsLocalId(shardIdx)) {
        db.Table<Schema::TableShardPartitionConfigs>().Key(shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::TableShardPartitionConfigs::PartitionConfig>(data));
    } else {
        db.Table<Schema::MigratedTableShardPartitionConfigs>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedTableShardPartitionConfigs::PartitionConfig>(data));
    }
}

void TSchemeShard::PersistPublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version) {
    IncrementPathDbRefCount(pathId, "publish path");

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::PublishingPaths>()
            .Key(txId, pathId.LocalPathId, version)
            .Update();
    } else {
        db.Table<Schema::MigratedPublishingPaths>()
            .Key(txId, pathId.OwnerId, pathId.LocalPathId, version)
            .Update();
    }
}

void TSchemeShard::PersistRemovePublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version) {
    DecrementPathDbRefCount(pathId, "remove publishing");

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::PublishingPaths>()
            .Key(txId, pathId.LocalPathId, version)
            .Delete();
    }

    db.Table<Schema::MigratedPublishingPaths>()
        .Key(txId, pathId.OwnerId, pathId.LocalPathId, version)
        .Delete();
}

void TSchemeShard::PersistLongIncrementalRestoreOp(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TLongIncrementalRestoreOp& op) {
    TString data;
    Y_PROTOBUF_SUPPRESS_NODISCARD op.SerializeToString(&data);

    db.Table<Schema::IncrementalRestoreOperations>()
        .Key(op.GetTxId())
        .Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreOperations::Operation>(data));
}

TTabletId TSchemeShard::GetGlobalHive() const {
    return TTabletId(AppData()->DomainsInfo->GetHive());
}

TShardIdx TSchemeShard::GetShardIdx(TTabletId tabletId) const {
    const auto* pIdx = TabletIdToShardIdx.FindPtr(tabletId);
    if (!pIdx) {
        return InvalidShardIdx;
    }

    Y_ABORT_UNLESS(*pIdx != InvalidShardIdx);
    return *pIdx;
}

TShardIdx TSchemeShard::MustGetShardIdx(TTabletId tabletId) const {
    auto shardIdx = GetShardIdx(tabletId);
    Y_VERIFY_S(shardIdx != InvalidShardIdx, "Cannot find shard idx for tablet " << tabletId);
    return shardIdx;
}

TTabletTypes::EType TSchemeShard::GetTabletType(TTabletId tabletId) const {
    const auto* pIdx = TabletIdToShardIdx.FindPtr(tabletId);
    if (!pIdx) {
        return TTabletTypes::Unknown;
    }

    Y_ABORT_UNLESS(*pIdx != InvalidShardIdx);
    const auto* pShardInfo = ShardInfos.FindPtr(*pIdx);
    if (!pShardInfo) {
        return TTabletTypes::Unknown;
    }

    return pShardInfo->TabletType;
}

TTabletId TSchemeShard::ResolveHive(TPathId pathId, EHiveSelection selection) const {
    if (!PathsById.contains(pathId)) {
        return GetGlobalHive();
    }

    TSubDomainInfo::TPtr subdomain = ResolveDomainInfo(pathId);

    // for paths inside subdomain and their shards we choose Hive according to that order: tenant, shared, global

    if (selection != EHiveSelection::IGNORE_TENANT && subdomain->GetTenantHiveID()) {
        return subdomain->GetTenantHiveID();
    }

    if (subdomain->GetSharedHive()) {
        return subdomain->GetSharedHive();
    }

    return GetGlobalHive();
}

TTabletId TSchemeShard::ResolveHive(TPathId pathId) const {
    return ResolveHive(pathId, EHiveSelection::ANY);
}

TTabletId TSchemeShard::ResolveHive(TShardIdx shardIdx) const {
    if (!ShardInfos.contains(shardIdx)) {
        return GetGlobalHive();
    }

    return ResolveHive(ShardInfos.at(shardIdx).PathId, EHiveSelection::ANY);
}

void TSchemeShard::DoShardsDeletion(const THashSet<TShardIdx>& shardIdxs, const TActorContext& ctx) {
    TMap<TTabletId, THashSet<TShardIdx>> shardsPerHive;
    for (TShardIdx shardIdx : shardIdxs) {
        TTabletId hiveToRequest = ResolveHive(shardIdx);

        shardsPerHive[hiveToRequest].emplace(shardIdx);
    }

    for (const auto& [hive, shards] : shardsPerHive) {
        ShardDeleter.SendDeleteRequests(hive, shards, ShardInfos, ctx);
    }
}

void TSchemeShard::DoDeleteSystemShards(const THashSet<TShardIdx>& shards, const TActorContext& ctx) {
    if (!shards.empty()) {
        ShardDeleter.SendDeleteRequests(GetGlobalHive(), shards, ShardInfos, ctx);
    }
}

NKikimrSchemeOp::TPathVersion TSchemeShard::GetPathVersion(const TPath& path) const {
    NKikimrSchemeOp::TPathVersion result;

    const auto pathEl = path.Base();
    const auto pathId = pathEl->PathId;

    if (pathEl->Dropped()) {
        result.SetGeneralVersion(Max<ui64>());
        return result;
    }

    ui64 generalVersion = 0;

    if (pathEl->IsCreateFinished()) {
        switch(pathEl->PathType) {
            case NKikimrSchemeOp::EPathType::EPathTypeDir:
                if (pathEl->IsRoot() && IsDomainSchemeShard) {
                    TSubDomainInfo::TPtr subDomain = SubDomains.at(pathId);
                    Y_ABORT_UNLESS(SubDomains.contains(pathId));
                    result.SetSubDomainVersion(subDomain->GetVersion());
                    result.SetSecurityStateVersion(subDomain->GetSecurityStateVersion());
                    generalVersion += result.GetSubDomainVersion();
                    generalVersion += result.GetSecurityStateVersion();

                    if (ui64 version = subDomain->GetDomainStateVersion()) {
                        result.SetSubDomainStateVersion(version);
                        generalVersion += version;
                    }
                }
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeSubDomain:
            case NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain: {
                Y_ABORT_UNLESS(!(pathEl->IsRoot() && IsDomainSchemeShard));

                Y_ABORT_UNLESS(SubDomains.contains(pathId));
                TSubDomainInfo::TPtr subDomain = SubDomains.at(pathId);
                result.SetSubDomainVersion(subDomain->GetVersion());
                result.SetSecurityStateVersion(subDomain->GetSecurityStateVersion());
                generalVersion += result.GetSubDomainVersion();
                generalVersion += result.GetSecurityStateVersion();

                if (ui64 version = subDomain->GetDomainStateVersion()) {
                    result.SetSubDomainStateVersion(version);
                    generalVersion += version;
                }
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeTable:
                Y_VERIFY_S(Tables.contains(pathId),
                           "no table with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                result.SetTableSchemaVersion(Tables.at(pathId)->AlterVersion);
                generalVersion += result.GetTableSchemaVersion();

                result.SetTablePartitionVersion(Tables.at(pathId)->PartitioningVersion);
                generalVersion += result.GetTablePartitionVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup:
                Y_ABORT_UNLESS(Topics.contains(pathId));
                result.SetPQVersion(Topics.at(pathId)->AlterVersion);
                generalVersion += result.GetPQVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeBlockStoreVolume:
                Y_ABORT_UNLESS(BlockStoreVolumes.contains(pathId));
                result.SetBSVVersion(BlockStoreVolumes.at(pathId)->AlterVersion);
                generalVersion += result.GetBSVVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeFileStore:
                Y_ABORT_UNLESS(FileStoreInfos.contains(pathId));
                result.SetFileStoreVersion(FileStoreInfos.at(pathId)->Version);
                generalVersion += result.GetFileStoreVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeKesus:
                Y_ABORT_UNLESS(KesusInfos.contains(pathId));
                result.SetKesusVersion(KesusInfos.at(pathId)->Version);
                generalVersion += result.GetKesusVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeRtmrVolume:
                result.SetRTMRVersion(1);
                generalVersion += result.GetRTMRVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeSolomonVolume:
                Y_ABORT_UNLESS(SolomonVolumes.contains(pathId));
                result.SetSolomonVersion(SolomonVolumes.at(pathId)->Version);
                generalVersion += result.GetSolomonVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
                Y_ABORT_UNLESS(Indexes.contains(pathId));
                result.SetTableIndexVersion(Indexes.at(pathId)->AlterVersion);
                generalVersion += result.GetTableIndexVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeColumnStore:
                Y_VERIFY_S(OlapStores.contains(pathId),
                           "no olap store with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                result.SetColumnStoreVersion(OlapStores.at(pathId)->GetAlterVersion());
                generalVersion += result.GetColumnStoreVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeColumnTable: {
                Y_VERIFY_S(ColumnTables.contains(pathId),
                           "no olap table with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                auto tableInfo = ColumnTables.at(pathId);

                result.SetColumnTableVersion(tableInfo->AlterVersion);
                generalVersion += result.GetColumnTableVersion();

                if (tableInfo->Description.HasSchema()) {
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchema().GetVersion());
                } else if (tableInfo->Description.HasSchemaPresetId() && tableInfo->GetOlapStorePathIdVerified()) {
                    Y_ABORT_UNLESS(OlapStores.contains(tableInfo->GetOlapStorePathIdVerified()));
                    auto& storeInfo = OlapStores.at(tableInfo->GetOlapStorePathIdVerified());
                    auto& preset = storeInfo->SchemaPresets.at(tableInfo->Description.GetSchemaPresetId());
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchemaPresetVersionAdj() + preset.GetVersion());
                } else {
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchemaPresetVersionAdj());
                }
                generalVersion += result.GetColumnTableSchemaVersion();

                if (tableInfo->Description.HasTtlSettings()) {
                    result.SetColumnTableTtlSettingsVersion(tableInfo->Description.GetTtlSettings().GetVersion());
                }
                generalVersion += result.GetColumnTableTtlSettingsVersion();

                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeCdcStream: {
                Y_ABORT_UNLESS(CdcStreams.contains(pathId));
                result.SetCdcStreamVersion(CdcStreams.at(pathId)->AlterVersion);
                generalVersion += result.GetCdcStreamVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeSequence: {
                auto it = Sequences.find(pathId);
                Y_ABORT_UNLESS(it != Sequences.end());
                result.SetSequenceVersion(it->second->AlterVersion);
                generalVersion += result.GetSequenceVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeReplication:
            case NKikimrSchemeOp::EPathType::EPathTypeTransfer: {
                auto it = Replications.find(pathId);
                Y_ABORT_UNLESS(it != Replications.end());
                result.SetReplicationVersion(it->second->AlterVersion);
                generalVersion += result.GetReplicationVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeBlobDepot:
                if (const auto it = BlobDepots.find(pathId); it != BlobDepots.end()) {
                    const ui64 version = it->second->AlterVersion;
                    result.SetBlobDepotVersion(version);
                    generalVersion += version;
                } else {
                    Y_FAIL_S("BlobDepot for path " << pathId << " not found");
                }
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeExternalTable: {
                auto it = ExternalTables.find(pathId);
                Y_ABORT_UNLESS(it != ExternalTables.end());
                result.SetExternalTableVersion(it->second->AlterVersion);
                generalVersion += result.GetExternalTableVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource: {
                auto it = ExternalDataSources.find(pathId);
                Y_ABORT_UNLESS(it != ExternalDataSources.end());
                result.SetExternalDataSourceVersion(it->second->AlterVersion);
                generalVersion += result.GetExternalDataSourceVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeView: {
                auto it = Views.find(pathId);
                Y_ABORT_UNLESS(it != Views.end());
                result.SetViewVersion(it->second->AlterVersion);
                generalVersion += result.GetViewVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeResourcePool: {
                auto it = ResourcePools.find(pathId);
                Y_ABORT_UNLESS(it != ResourcePools.end());
                result.SetResourcePoolVersion(it->second->AlterVersion);
                generalVersion += result.GetResourcePoolVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeBackupCollection: {
                auto it = BackupCollections.find(pathId);
                Y_ABORT_UNLESS(it != BackupCollections.end());
                result.SetBackupCollectionVersion(it->second->AlterVersion);
                generalVersion += result.GetBackupCollectionVersion();
                break;
            }

            case NKikimrSchemeOp::EPathType::EPathTypeSysView: {
                auto it = SysViews.find(pathId);
                Y_ABORT_UNLESS(it != SysViews.end());
                result.SetSysViewVersion(it->second->AlterVersion);
                generalVersion += result.GetSysViewVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeStreamingQuery: {
                const auto it = StreamingQueries.find(pathId);
                Y_ABORT_UNLESS(it != StreamingQueries.end());
                result.SetStreamingQueryVersion(it->second->AlterVersion);
                generalVersion += result.GetStreamingQueryVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeTestShardSet: {
                const auto it = TestShardSets.find(pathId);
                Y_ABORT_UNLESS(it != TestShardSets.end());
                result.SetTestShardSetVersion(it->second->AlterVersion);
                generalVersion += result.GetTestShardSetVersion();
                break;
            }

            case NKikimrSchemeOp::EPathType::EPathTypeSecret: {
                auto it = Secrets.find(pathId);
                Y_ABORT_UNLESS(it != Secrets.end());
                result.SetSecretVersion(it->second->AlterVersion);
                generalVersion += result.GetSecretVersion();
                break;
            }

            case NKikimrSchemeOp::EPathType::EPathTypeInvalid: {
                Y_UNREACHABLE();
            }
        }
    }


    result.SetChildrenVersion(pathEl->DirAlterVersion); //not only childrens but also acl children's version increases it
    generalVersion += result.GetChildrenVersion();

    result.SetUserAttrsVersion(pathEl->UserAttrs->AlterVersion);
    generalVersion += result.GetUserAttrsVersion();

    result.SetACLVersion(pathEl->ACLVersion); // do not add ACL version to the generalVersion here
    result.SetEffectiveACLVersion(path.GetEffectiveACLVersion()); // ACL version is added to generalVersion here
    generalVersion += result.GetEffectiveACLVersion();

    result.SetGeneralVersion(generalVersion);

    return result;
}

ui64 TSchemeShard::GetAliveChildren(TPathElement::TPtr pathEl, const std::optional<TPathElement::EPathType>& type) const {
    if (!type) {
        return pathEl->GetAliveChildren();
    }

    ui64 count = 0;
    for (const auto& [_, pathId] : pathEl->GetChildren()) {
        Y_ABORT_UNLESS(PathsById.contains(pathId));
        auto childPath = PathsById.at(pathId);

        if (childPath->Dropped()) {
            continue;
        }

        count += ui64(childPath->PathType == *type);
    }

    return count;
}

TActorId TSchemeShard::TPipeClientFactory::CreateClient(const TActorContext& ctx, ui64 tabletId, const NTabletPipe::TClientConfig& pipeConfig){
    auto clientId = Self->Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, pipeConfig));
    switch (Self->GetTabletType(TTabletId(tabletId))) {
        case ETabletType::SequenceShard: {
            // Every time we create a new pipe to sequenceshard we use a new round
            auto round = Self->NextRound();
            NTabletPipe::SendData(
                ctx.SelfID, clientId,
                new NSequenceShard::TEvSequenceShard::TEvMarkSchemeShardPipe(
                    Self->TabletID(),
                    round.Generation,
                    round.Round));
            break;
        }
        default: {
            // Other tablets don't need anything special
            break;
        }
    }
    return clientId;
}

TSchemeShard::TSchemeShard(const TActorId &tablet, TTabletStorageInfo *info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , AllowConditionalEraseOperations(1, 0, 1)
    , AllowServerlessStorageBilling(0, 0, 1)
    , DisablePublicationsOfDropping(0, 0, 1)
    , FillAllocatePQ(0, 0, 1)
    , TolerateOrphanedPaths(0, 0, 1)
    , SplitSettings()
    , IsReadOnlyMode(false)
    , ParentDomainLink(this)
    , SubDomainsLinks(this)
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(
        new NTabletPipe::TBoundedClientCacheConfig(),
        GetPipeClientConfig(),
        new TPipeClientFactory(this)))
    , PipeTracker(*PipeClientCache)
    , BackgroundCompactionStarter(this)
    , BorrowedCompactionStarter(this)
    , ForcedCompactionStarter(this)
    , BackgroundCleaningStarter(this)
    , ShardDeleter(info->TabletID)
    , TableStatsQueue(this,
            COUNTER_STATS_QUEUE_SIZE,
            COUNTER_STATS_WRITTEN,
            COUNTER_STATS_BATCH_LATENCY)
    , TopicStatsQueue(this,
            COUNTER_PQ_STATS_QUEUE_SIZE,
            COUNTER_PQ_STATS_WRITTEN,
            COUNTER_PQ_STATS_BATCH_LATENCY)
    , AllowDataColumnForIndexTable(0, 0, 1)
    , LoginProvider(NLogin::TPasswordComplexity({
            .MinLength = AppData()->AuthConfig.GetPasswordComplexity().GetMinLength(),
            .MinLowerCaseCount = AppData()->AuthConfig.GetPasswordComplexity().GetMinLowerCaseCount(),
            .MinUpperCaseCount = AppData()->AuthConfig.GetPasswordComplexity().GetMinUpperCaseCount(),
            .MinNumbersCount = AppData()->AuthConfig.GetPasswordComplexity().GetMinNumbersCount(),
            .MinSpecialCharsCount = AppData()->AuthConfig.GetPasswordComplexity().GetMinSpecialCharsCount(),
            .SpecialChars = AppData()->AuthConfig.GetPasswordComplexity().GetSpecialChars(),
            .CanContainUsername = AppData()->AuthConfig.GetPasswordComplexity().GetCanContainUsername()
        }), {
            .AttemptThreshold = AppData()->AuthConfig.GetAccountLockout().GetAttemptThreshold(),
            .AttemptResetDuration = AppData()->AuthConfig.GetAccountLockout().GetAttemptResetDuration()
        })
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
                            ESimpleCounters_descriptor,
                            ECumulativeCounters_descriptor,
                            EPercentileCounters_descriptor,
                            ETxTypes_descriptor
                            >());
    TabletCounters = TabletCountersPtr.Get();

    SelfPinger = new TSelfPinger(SelfTabletId(), TabletCounters);
    BackgroundSessionsManager = std::make_shared<NKikimr::NOlap::NBackground::TSessionsManager>(std::make_shared<NBackground::TAdapter>(tablet, NKikimr::NOlap::TTabletId(TabletID()), *this));
}

const TDomainsInfo::TDomain& TSchemeShard::GetDomainDescription(const TActorContext &ctx) const {
    return *AppData(ctx)->DomainsInfo->GetDomain();
}

const NKikimrConfig::TDomainsConfig& TSchemeShard::GetDomainsConfig() {
    Y_ABORT_UNLESS(AppData());
    return AppData()->DomainsConfig;
}

NKikimrSubDomains::TProcessingParams TSchemeShard::CreateRootProcessingParams(const TActorContext &ctx) {
    const auto& domain = GetDomainDescription(ctx);

    Y_ABORT_UNLESS(domain.Coordinators.size());
    return ExtractProcessingParams(domain);
}

NTabletPipe::TClientConfig TSchemeShard::GetPipeClientConfig() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .MinRetryTime = TDuration::MilliSeconds(50),
        .MaxRetryTime = TDuration::Seconds(2),
    };
    return config;
}


void TSchemeShard::FillTableSchemaVersion(ui64 tableSchemaVersion, NKikimrSchemeOp::TTableDescription* tableDescr) const {
    tableDescr->SetTableSchemaVersion(tableSchemaVersion);
}

void TSchemeShard::BreakTabletAndRestart(const TActorContext &ctx) {
    Become(&TThis::BrokenState);
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
}

bool TSchemeShard::IsSchemeShardConfigured() const {
    Y_ABORT_UNLESS(InitState != TTenantInitState::InvalidState);
    return InitState == TTenantInitState::Done || InitState == TTenantInitState::ReadOnlyPreview;
}

void TSchemeShard::Die(const TActorContext &ctx) {
    ctx.Send(SchemeBoardPopulator, new TEvents::TEvPoisonPill());
    ctx.Send(TxAllocatorClient, new TEvents::TEvPoisonPill());
    ctx.Send(SysPartitionStatsCollector, new TEvents::TEvPoisonPill());

    if (TabletMigrator) {
        ctx.Send(TabletMigrator, new TEvents::TEvPoisonPill());
    }
    if (LocalIndexMigratorId) {
        ctx.Send(LocalIndexMigratorId, new TEvents::TEvPoisonPill());
    }
    for (TActorId schemeUploader : RunningExportSchemeUploaders) {
        ctx.Send(schemeUploader, new TEvents::TEvPoisonPill());
    }
    for (TActorId schemeGetter : RunningImportSchemeGetters) {
        ctx.Send(schemeGetter, new TEvents::TEvPoisonPill());
    }
    for (TActorId schemeQueryExecutor : RunningImportSchemeQueryExecutors) {
        ctx.Send(schemeQueryExecutor, new TEvents::TEvPoisonPill());
    }
    for (TActorId continuousBackupCleaner : RunningContinuousBackupCleaners) {
        ctx.Send(continuousBackupCleaner, new TEvents::TEvPoisonPill());
    }

    IndexBuildPipes.Shutdown(ctx);
    SetColumnConstraintPipes.Shutdown(ctx);
    IncrementalRestorePipes.Shutdown(ctx);
    CdcStreamScanPipes.Shutdown(ctx);
    ShardDeleter.Shutdown(ctx);
    ParentDomainLink.Shutdown(ctx);

    if (SAPipeClientId) {
        NTabletPipe::CloseClient(SelfId(), SAPipeClientId);
    }

    PipeClientCache->Detach(ctx);

    if (BackgroundCompactionQueue)
        BackgroundCompactionQueue->Shutdown(ctx);

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->Shutdown(ctx);
    }

    if (ForcedCompactionQueue) {
        ForcedCompactionQueue->Shutdown(ctx);
    }

    ClearTempDirsState();
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Shutdown(ctx);
    }

    return IActor::Die(ctx);
}


} // namespace NSchemeShard
} // namespace NKikimr
