#include "schemeshard__init_tx.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

bool TSchemeShard::TTxInit::LoadPaths(NIceDb::TNiceDb& db, TPathRows& pathRows) const {
        {
            auto rows = db.Table<Schema::MigratedPaths>().Range().Select();
            if (!rows.IsReady()) {
                return false;
            }
            while (!rows.EndOfSet()) {
                const auto pathId = TPathId(
                    rows.GetValue<Schema::MigratedPaths::OwnerPathId>(),
                    rows.GetValue<Schema::MigratedPaths::LocalPathId>()
                );
                const auto parentPathId = TPathId(
                    rows.GetValue<Schema::MigratedPaths::ParentOwnerId>(),
                    rows.GetValue<Schema::MigratedPaths::ParentLocalId>()
                );
                pathRows.push_back(MakePathRec<Schema::MigratedPaths>(pathId, parentPathId, rows));

                if (!rows.Next()) {
                    return false;
                }
            }
        }
        {
            auto rows = db.Table<Schema::Paths>().Range().Select();
            if (!rows.IsReady()) {
                return false;
            }
            while (!rows.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rows.GetValue<Schema::Paths::Id>());
                const auto parentPathId = TPathId(
                    rows.GetValueOrDefault<Schema::Paths::ParentOwnerId>(Self->TabletID()),
                    rows.GetValue<Schema::Paths::ParentId>()
                );

                if (pathId.LocalPathId == 0) {
                    const auto name = rows.GetValue<Schema::Paths::Name>();
                    // Skip special incompatibility marker
                    Y_VERIFY_S(parentPathId.LocalPathId == 0 && name == "/incompatible/",
                        "Unexpected row PathId# " << pathId << " ParentPathId# " << parentPathId << " Name# " << name);

                    if (!rows.Next()) {
                        return false;
                    }

                    continue;
                }

                if (pathId == parentPathId) {
                    pathRows.push_front(MakePathRec<Schema::Paths>(pathId, parentPathId, rows));
                } else {
                    pathRows.push_back(MakePathRec<Schema::Paths>(pathId, parentPathId, rows));
                }

                if (!rows.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadUserAttrs(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const {
        {
            auto rowSet = db.Table<Schema::UserAttributes>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::UserAttributes::PathId>());
                userAttrsRows.push_back(MakeUserAttrsRec<Schema::UserAttributes>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedUserAttributes>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedUserAttributes::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedUserAttributes::LocalPathId>()
                );
                userAttrsRows.push_back(MakeUserAttrsRec<Schema::MigratedUserAttributes>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadUserAttrsAlterData(NIceDb::TNiceDb& db, TUserAttrsRows& userAttrsRows) const {
        {
            auto rowSet = db.Table<Schema::UserAttributesAlterData>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::UserAttributesAlterData::PathId>());
                userAttrsRows.push_back(MakeUserAttrsRec<Schema::UserAttributesAlterData>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedUserAttributesAlterData>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedUserAttributesAlterData::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedUserAttributesAlterData::LocalPathId>()
                );
                userAttrsRows.push_back(MakeUserAttrsRec<Schema::MigratedUserAttributesAlterData>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadTables(NIceDb::TNiceDb& db, TTableRows& tableRows) const {
        {
            auto rowSet = db.Table<Schema::Tables>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::Tables::TabId>());
                tableRows.push_back(MakeTableRec<Schema::Tables>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedTables>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedTables::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedTables::LocalPathId>()
                );
                tableRows.push_back(MakeTableRec<Schema::MigratedTables>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadColumns(NIceDb::TNiceDb& db, TColumnRows& columnRows) const {
        {
            auto rowSet = db.Table<Schema::Columns>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::Columns::TabId>());
                columnRows.push_back(MakeColumnRec<Schema::Columns>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedColumns>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedColumns::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedColumns::LocalPathId>()
                );
                columnRows.push_back(MakeColumnRec<Schema::MigratedColumns>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadColumnsAlters(NIceDb::TNiceDb& db, TColumnRows& columnRows) const {
        {
            auto rowSet = db.Table<Schema::ColumnAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::ColumnAlters::TabId>());
                columnRows.push_back(MakeColumnRec<Schema::ColumnAlters>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedColumnAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedColumnAlters::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedColumnAlters::LocalPathId>()
                );
                columnRows.push_back(MakeColumnRec<Schema::MigratedColumnAlters>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadTablePartitions(NIceDb::TNiceDb& db, TTablePartitionsRows& partitionsRows) const {
        {
            auto rowSet = db.Table<Schema::TablePartitions>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::TablePartitions::TabId>());
                const auto datashardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::TablePartitions::DatashardIdx>());
                partitionsRows.push_back(MakeTablePartitionRec<Schema::TablePartitions>(pathId, datashardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedTablePartitions>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedTablePartitions::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedTablePartitions::LocalPathId>()
                );
                const auto datashardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedTablePartitions::OwnerShardIdx>(),
                    rowSet.GetValue<Schema::MigratedTablePartitions::LocalShardIdx>()
                );
                partitionsRows.push_back(MakeTablePartitionRec<Schema::MigratedTablePartitions>(pathId, datashardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        // We need to sort partitions by PathId/PartitionId due to incompatible change 1
        std::sort(partitionsRows.begin(), partitionsRows.end());

        return true;
    }

bool TSchemeShard::TTxInit::LoadTableShardPartitionConfigs(NIceDb::TNiceDb& db, TTableShardPartitionConfigRows& partitionsRows) const {
        {
            auto rowSet = db.Table<Schema::TableShardPartitionConfigs>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::TableShardPartitionConfigs::ShardIdx>());
                partitionsRows.push_back(MakeTableShardPartitionConfigRec<Schema::TableShardPartitionConfigs>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedTableShardPartitionConfigs>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedTableShardPartitionConfigs::OwnerShardIdx>(),
                    rowSet.GetValue<Schema::MigratedTableShardPartitionConfigs::LocalShardIdx>()
                );
                partitionsRows.push_back(MakeTableShardPartitionConfigRec<Schema::MigratedTableShardPartitionConfigs>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadPublications(NIceDb::TNiceDb& db, TPublicationsRows& publicationsRows) const {
        {
            auto rowSet = db.Table<Schema::PublishingPaths>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::PublishingPaths::PathId>());
                publicationsRows.push_back(MakePublicationRec<Schema::PublishingPaths>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedPublishingPaths>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedPublishingPaths::PathOwnerId>(),
                    rowSet.GetValue<Schema::MigratedPublishingPaths::LocalPathId>()
                );
                publicationsRows.push_back(MakePublicationRec<Schema::MigratedPublishingPaths>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadShardsToDelete(NIceDb::TNiceDb& db, TShardsToDeleteRows& shardsToDelete) const {
        {
            auto rowSet = db.Table<Schema::ShardsToDelete>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::ShardsToDelete::ShardIdx>());
                shardsToDelete.emplace_back(shardIdx);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedShardsToDelete>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedShardsToDelete::ShardOwnerId>(),
                    rowSet.GetValue<Schema::MigratedShardsToDelete::ShardLocalIdx>()
                );
                shardsToDelete.emplace_back(shardIdx);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadSystemShardsToDelete(NIceDb::TNiceDb& db, TShardsToDeleteRows& shardsToDelete) const {
        {
            auto rowSet = db.Table<Schema::SystemShardsToDelete>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::SystemShardsToDelete::ShardIdx>());
                shardsToDelete.emplace_back(shardIdx);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadTxShards(NIceDb::TNiceDb& db, TTxShardsRows& txShards) const {
        {
            auto rowset = db.Table<Schema::TxShards>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                const auto operationId = TOperationId(rowset.GetValue<Schema::TxShards::TxId>(), 0);
                txShards.push_back(MakeTxShardRec<Schema::TxShards>(operationId, rowset));

                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowset = db.Table<Schema::TxShardsV2>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                const auto operationId = TOperationId(
                    rowset.GetValue<Schema::TxShardsV2::TxId>(),
                    rowset.GetValue<Schema::TxShardsV2::TxPartId>()
                );
                txShards.push_back(MakeTxShardRec<Schema::TxShardsV2>(operationId, rowset));

                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowset = db.Table<Schema::MigratedTxShards>().Range().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                const auto operationId = TOperationId(
                    rowset.GetValue<Schema::MigratedTxShards::TxId>(),
                    rowset.GetValue<Schema::MigratedTxShards::TxPartId>()
                );
                const auto shardIdx = TShardIdx(
                    rowset.GetValue<Schema::MigratedTxShards::ShardOwnerId>(),
                    rowset.GetValue<Schema::MigratedTxShards::ShardLocalIdx>()
                );
                txShards.push_back(MakeTxShardRec<Schema::MigratedTxShards>(operationId, shardIdx, rowset));

                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        Sort(txShards);
        auto last = Unique(txShards.begin(), txShards.end());
        txShards.erase(last, txShards.end());

        return true;
    }

bool TSchemeShard::TTxInit::LoadShards(NIceDb::TNiceDb& db, TShardsRows& shards) const {
        {
            auto rowSet = db.Table<Schema::Shards>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::Shards::ShardIdx>());
                const auto pathId = TPathId(
                    rowSet.GetValueOrDefault<Schema::Shards::OwnerPathId>(Self->TabletID()),
                    rowSet.GetValue<Schema::Shards::PathId>()
                );
                shards.push_back(MakeShardsRec<Schema::Shards>(shardIdx, pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedShards>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedShards::OwnerShardId>(),
                    rowSet.GetValue<Schema::MigratedShards::LocalShardId>()
                );
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedShards::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedShards::LocalPathId>()
                );
                shards.push_back(MakeShardsRec<Schema::MigratedShards>(shardIdx, pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadSharedShards(NIceDb::TNiceDb& db) const {
        auto rowSet = db.Table<Schema::SharedShards>().Range().Select();
        if (!rowSet.IsReady()) {
            return false;
        }
        while (!rowSet.EndOfSet()) {
            const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::SharedShards::ShardIdx>());
            const auto pathId = TPathId(
                rowSet.GetValueOrDefault<Schema::SharedShards::OwnerPathId>(Self->TabletID()),
                rowSet.GetValue<Schema::SharedShards::LocalPathId>()
            );
            const auto currentTxId = TTxId(rowSet.GetValueOrDefault<Schema::SharedShards::LastTxId>(0));
            Self->SharedShards[shardIdx][pathId] = currentTxId;
            if (!rowSet.Next()) {
                return false;
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadBackupSettings(NIceDb::TNiceDb& db, TBackupSettingsRows& settings) const {
        {
            auto rowSet = db.Table<Schema::BackupSettings>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::BackupSettings::PathId>());
                settings.push_back(MakeBackupSettingsRec<Schema::BackupSettings>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedBackupSettings>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedBackupSettings::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedBackupSettings::LocalPathId>()
                );
                settings.push_back(MakeBackupSettingsRec<Schema::MigratedBackupSettings>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadBackupRestoreHistory(NIceDb::TNiceDb& db, TCompletedBackupRestoreRows& history) const {
        {
            auto rowSet = db.Table<Schema::CompletedBackups>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::CompletedBackups::PathId>());
                history.push_back(MakeCompletedBackupRestoreRec<Schema::CompletedBackups>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedCompletedBackups>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedCompletedBackups::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedCompletedBackups::LocalPathId>()
                );
                history.push_back(MakeCompletedBackupRestoreRec<Schema::MigratedCompletedBackups>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadBackupStatusesImpl(TShardBackupStatusRows& statuses, T& byShardBackupStatus, U& byMigratedShardBackupStatus, V& byTxShardStatus) const {
        {
            T& rowSet = byShardBackupStatus;
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.template GetValue<Schema::ShardBackupStatus::ShardIdx>());
                statuses.push_back(MakeShardBackupStatusRec<Schema::ShardBackupStatus>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            U& rowSet = byMigratedShardBackupStatus;
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = TShardIdx(
                    rowSet.template GetValue<Schema::MigratedShardBackupStatus::OwnerShardId>(),
                    rowSet.template GetValue<Schema::MigratedShardBackupStatus::LocalShardId>()
                );
                statuses.push_back(MakeShardBackupStatusRec<Schema::MigratedShardBackupStatus>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            V& rowSet = byTxShardStatus;
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                auto txId = rowSet.template GetValue<Schema::TxShardStatus::TxId>();
                auto shardIdx = TShardIdx(
                    rowSet.template GetValue<Schema::TxShardStatus::OwnerShardId>(),
                    rowSet.template GetValue<Schema::TxShardStatus::LocalShardId>()
                );
                auto success = rowSet.template GetValue<Schema::TxShardStatus::Success>();
                auto error = rowSet.template GetValue<Schema::TxShardStatus::Error>();
                auto bytes = rowSet.template GetValue<Schema::TxShardStatus::BytesProcessed>();
                auto rows = rowSet.template GetValue<Schema::TxShardStatus::RowsProcessed>();

                statuses.emplace_back(txId, shardIdx, success, error, bytes, rows);

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadBackupStatuses(NIceDb::TNiceDb& db, TShardBackupStatusRows& statuses) const {
        auto byShardBackupStatus = db.Table<Schema::ShardBackupStatus>().Range().Select();
        auto byMigratedShardBackupStatus = db.Table<Schema::MigratedShardBackupStatus>().Range().Select();
        auto byTxShardStatus = db.Table<Schema::TxShardStatus>().Range().Select();

        return LoadBackupStatusesImpl(statuses, byShardBackupStatus, byMigratedShardBackupStatus, byTxShardStatus);
    }

bool TSchemeShard::TTxInit::LoadTableIndexes(NIceDb::TNiceDb& db, TTableIndexRows& tableIndexes) const {
        {
            auto rowSet = db.Table<Schema::TableIndex>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(TLocalPathId(rowSet.GetValue<Schema::TableIndex::PathId>()));
                tableIndexes.emplace_back(
                    pathId,
                    rowSet.GetValue<Schema::TableIndex::AlterVersion>(),
                    rowSet.GetValue<Schema::TableIndex::IndexType>(),
                    rowSet.GetValue<Schema::TableIndex::State>(),
                    rowSet.GetValue<Schema::TableIndex::Description>()
                );

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedTableIndex>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    TOwnerId(rowSet.GetValue<Schema::MigratedTableIndex::OwnerPathId>()),
                    TLocalPathId(rowSet.GetValue<Schema::MigratedTableIndex::LocalPathId>())
                );
                tableIndexes.emplace_back(
                    pathId,
                    rowSet.GetValue<Schema::MigratedTableIndex::AlterVersion>(),
                    rowSet.GetValue<Schema::MigratedTableIndex::IndexType>(),
                    rowSet.GetValue<Schema::MigratedTableIndex::State>(),
                    TString{}
                );

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadTableIndexKeys(NIceDb::TNiceDb& db, TTableIndexKeyRows& tableIndexKeys) const {
        {
            auto rowSet = db.Table<Schema::TableIndexKeys>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(TLocalPathId(rowSet.GetValue<Schema::TableIndexKeys::PathId>()));
                tableIndexKeys.push_back(MakeTableIndexColRec<Schema::TableIndexKeys>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedTableIndexKeys>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedTableIndexKeys::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedTableIndexKeys::LocalPathId>()
                );
                tableIndexKeys.push_back(MakeTableIndexColRec<Schema::MigratedTableIndexKeys>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadTableIndexDataColumns(NIceDb::TNiceDb& db, TTableIndexDataRows& tableIndexData) const {
        auto rowSet = db.Table<Schema::TableIndexDataColumns>().Range().Select();
        if (!rowSet.IsReady()) {
            return false;
        }
        while (!rowSet.EndOfSet()) {
            const auto pathId = TPathId(
                rowSet.GetValue<Schema::TableIndexDataColumns::PathOwnerId>(),
                rowSet.GetValue<Schema::TableIndexDataColumns::PathLocalId>()
            );
            auto id = rowSet.GetValue<Schema::TableIndexDataColumns::DataColumnId>();
            auto name = rowSet.GetValue<Schema::TableIndexDataColumns::DataColumnName>();
            tableIndexData.emplace_back(pathId, id, name);

            if (!rowSet.Next()) {
                return false;
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadChannelBindings(NIceDb::TNiceDb& db, TChannelBindingRows& channeldBindings) const {
        {
            auto rowSet = db.Table<Schema::ChannelsBinding>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = Self->MakeLocalId(rowSet.GetValue<Schema::ChannelsBinding::ShardId>());
                channeldBindings.push_back(MakeChannelBindingRec<Schema::ChannelsBinding>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedChannelsBinding>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto shardIdx = TShardIdx(
                    rowSet.GetValue<Schema::MigratedChannelsBinding::OwnerShardId>(),
                    rowSet.GetValue<Schema::MigratedChannelsBinding::LocalShardId>()
                );
                channeldBindings.push_back(MakeChannelBindingRec<Schema::MigratedChannelsBinding>(shardIdx, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadKesusInfos(NIceDb::TNiceDb& db, TKesusInfosRows& kesusInfosData) const {
        {
            auto rowSet = db.Table<Schema::KesusInfos>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::KesusInfos::PathId>());
                kesusInfosData.push_back(MakeKesusInfosRec<Schema::KesusInfos>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedKesusInfos>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedKesusInfos::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedKesusInfos::LocalPathId>()
                );
                kesusInfosData.push_back(MakeKesusInfosRec<Schema::MigratedKesusInfos>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

bool TSchemeShard::TTxInit::LoadKesusAlters(NIceDb::TNiceDb& db, TKesusAlterRows& kesusAlterData) const {
        {
            auto rowSet = db.Table<Schema::KesusAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = Self->MakeLocalId(rowSet.GetValue<Schema::KesusAlters::PathId>());
                kesusAlterData.push_back(MakeKesusInfosRec<Schema::KesusAlters>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }
        {
            auto rowSet = db.Table<Schema::MigratedKesusAlters>().Range().Select();
            if (!rowSet.IsReady()) {
                return false;
            }
            while (!rowSet.EndOfSet()) {
                const auto pathId = TPathId(
                    rowSet.GetValue<Schema::MigratedKesusAlters::OwnerPathId>(),
                    rowSet.GetValue<Schema::MigratedKesusAlters::LocalPathId>()
                );
                kesusAlterData.push_back(MakeKesusInfosRec<Schema::MigratedKesusAlters>(pathId, rowSet));

                if (!rowSet.Next()) {
                    return false;
                }
            }
        }

        return true;
    }


} // namespace NSchemeShard
} // namespace NKikimr
