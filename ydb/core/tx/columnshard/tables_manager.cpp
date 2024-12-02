#include "columnshard_schema.h"
#include "tables_manager.h"

#include "engines/column_engine_logs.h"
#include "transactions/transactions/tx_add_sharding_info.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/tiering/manager.h>

#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard {

void TSchemaPreset::Deserialize(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) {
    Id = presetProto.GetId();
    Name = presetProto.GetName();
}

bool TTablesManager::FillMonitoringReport(NTabletFlatExecutor::TTransactionContext& txc, NJson::TJsonValue& json) {
    NIceDb::TNiceDb db(txc.DB);
    {
        auto& schemaJson = json.InsertValue("schema_versions", NJson::JSON_ARRAY);
        auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            NProtobufJson::Proto2Json(info, schemaJson.AppendValue(NJson::JSON_MAP));

            if (!rowset.Next()) {
                return false;
            }
        }
    }
    json.InsertValue("tables_count", Tables.size());
    json.InsertValue("presets_count", SchemaPresetsIds.size());
    json.InsertValue("to_drop_count", PathsToDrop.size());
    return true;
}

bool TTablesManager::InitFromDB(NIceDb::TNiceDb& db) {
    THashMap<ui32, TSchemaPreset> schemaPresets;
    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->TableLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::Tables");
        auto rowset = db.Table<Schema::TableInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        while (!rowset.EndOfSet()) {
            TTableInfo table;
            if (!table.InitFromDB(rowset)) {
                timer.AddLoadingFail();
                return false;
            }
            if (table.IsDropped()) {
                PathsToDrop.insert(table.GetPathId());
            }

            AFL_VERIFY(Tables.emplace(table.GetPathId(), std::move(table)).second);

            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    bool isFakePresetOnly = true;
    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->SchemaPresetLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::SchemaPresets");
        auto rowset = db.Table<Schema::SchemaPresetInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        while (!rowset.EndOfSet()) {
            TSchemaPreset preset;
            preset.InitFromDB(rowset);

            if (preset.IsStandaloneTable()) {
                Y_VERIFY_S(!preset.GetName(), "Preset name: " + preset.GetName());
            } else {
                Y_VERIFY_S(preset.GetName() == "default", "Preset name: " + preset.GetName());
                isFakePresetOnly = false;
            }
            AFL_VERIFY(schemaPresets.emplace(preset.GetId(), preset).second);
            AFL_VERIFY(SchemaPresetsIds.emplace(preset.GetId()).second);
            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->TableVersionsLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::Versions");
        auto rowset = db.Table<Schema::TableVersionInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        THashMap<ui64, NOlap::TSnapshot> lastVersion;
        while (!rowset.EndOfSet()) {
            const ui64 pathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
            Y_ABORT_UNLESS(Tables.contains(pathId));
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::TableVersionInfo::SinceStep>(), rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());

            auto& table = Tables[pathId];
            NKikimrTxColumnShard::TTableVersionInfo versionInfo;
            Y_ABORT_UNLESS(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "load_table_version")("path_id", pathId)("snapshot", version);
            Y_ABORT_UNLESS(schemaPresets.contains(versionInfo.GetSchemaPresetId()));

            if (!table.IsDropped()) {
                auto& ttlSettings = versionInfo.GetTtlSettings();
                auto vIt = lastVersion.find(pathId);
                if (vIt == lastVersion.end()) {
                    vIt = lastVersion.emplace(pathId, version).first;
                }
                if (vIt->second <= version) {
                    if (ttlSettings.HasEnabled()) {
                        TTtl::TDescription description(ttlSettings.GetEnabled());
                        Ttl.SetPathTtl(pathId, std::move(description));
                    } else {
                        Ttl.DropPathTtl(pathId);
                    }
                    vIt->second = version;
                }
            }
            table.AddVersion(version);
            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->SchemaPresetVersionsLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::PresetVersions");
        auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
            Y_ABORT_UNLESS(schemaPresets.contains(id));
            auto& preset = schemaPresets[id];
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(), rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());

            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "load_preset")("preset_id", id)("snapshot", version)(
                "version", info.HasSchema() ? info.GetSchema().GetVersion() : -1);
            preset.AddVersion(version, info);
            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    TMemoryProfileGuard g("TTablesManager/InitFromDB::Other");
    for (auto& [id, preset] : schemaPresets) {
        if (isFakePresetOnly) {
            Y_ABORT_UNLESS(id == 0);
        } else {
            Y_ABORT_UNLESS(id > 0);
        }
        for (auto it = preset.MutableVersionsById().begin(); it != preset.MutableVersionsById().end();) {
            const auto version = it->first;
            const auto& schemaInfo = it->second;
            AFL_VERIFY(schemaInfo.HasSchema());
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "index_schema")("preset_id", id)("snapshot", version)(
                "version", schemaInfo.GetSchema().GetVersion());
            NOlap::IColumnEngine::TSchemaInitializationData schemaInitializationData(schemaInfo);
            if (!PrimaryIndex) {
                PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(TabletId, DataAccessorsManager, StoragesManager,
                    preset.GetMinVersionForId(schemaInfo.GetSchema().GetVersion()), schemaInitializationData);
            } else {
                PrimaryIndex->RegisterSchemaVersion(preset.GetMinVersionForId(schemaInfo.GetSchema().GetVersion()), schemaInitializationData);
            }
            it = preset.MutableVersionsById().erase(it);
        }
    }
    for (auto&& i : Tables) {
        PrimaryIndex->RegisterTable(i.first);
    }
    return true;
}

bool TTablesManager::HasTable(const ui64 pathId, bool withDeleted) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end()) {
        return false;
    }
    if (it->second.IsDropped()) {
        return withDeleted;
    }
    return true;
}

bool TTablesManager::IsReadyForWrite(const ui64 pathId) const {
    return HasPrimaryIndex() && HasTable(pathId);
}

bool TTablesManager::HasPreset(const ui32 presetId) const {
    return SchemaPresetsIds.contains(presetId);
}

const TTableInfo& TTablesManager::GetTable(const ui64 pathId) const {
    Y_ABORT_UNLESS(HasTable(pathId));
    return Tables.at(pathId);
}

ui64 TTablesManager::GetMemoryUsage() const {
    ui64 memory = Tables.size() * sizeof(TTableInfo) + PathsToDrop.size() * sizeof(ui64) + Ttl.PathsCount() * sizeof(TTtl::TDescription);
    if (PrimaryIndex) {
        memory += PrimaryIndex->MemoryUsage();
    }
    return memory;
}

void TTablesManager::DropTable(const ui64 pathId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    AFL_VERIFY(Tables.contains(pathId));
    auto& table = Tables[pathId];
    table.SetDropVersion(version);
    PathsToDrop.insert(pathId);
    Ttl.DropPathTtl(pathId);
    Schema::SaveTableDropVersion(db, pathId, version.GetPlanStep(), version.GetTxId());
}

void TTablesManager::DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    AFL_VERIFY(SchemaPresetsIds.contains(presetId));
    SchemaPresetsIds.erase(presetId);
    Schema::SaveSchemaPresetDropVersion(db, presetId, version);
}

void TTablesManager::RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db) {
    Y_ABORT_UNLESS(!HasTable(table.GetPathId()));
    Y_ABORT_UNLESS(table.IsEmpty());

    Schema::SaveTableInfo(db, table.GetPathId(), table.GetTieringUsage());
    const ui64 pathId = table.GetPathId();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("method", "RegisterTable")("path_id", pathId);
    AFL_VERIFY(Tables.emplace(pathId, std::move(table)).second)("path_id", pathId)("size", Tables.size());
    if (PrimaryIndex) {
        PrimaryIndex->RegisterTable(pathId);
    }
}

bool TTablesManager::RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db) {
    if (SchemaPresetsIds.contains(schemaPreset.GetId())) {
        return false;
    }
    SchemaPresetsIds.emplace(schemaPreset.GetId());
    Schema::SaveSchemaPresetInfo(db, schemaPreset.GetId(), schemaPreset.GetName());
    return true;
}

void TTablesManager::AddSchemaVersion(const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema,
    NIceDb::TNiceDb& db, std::shared_ptr<TTiersManager>& manager) {
    Y_ABORT_UNLESS(SchemaPresetsIds.contains(presetId));

    TSchemaPreset::TSchemaPresetVersionInfo versionInfo;
    versionInfo.SetId(presetId);
    versionInfo.SetSinceStep(version.GetPlanStep());
    versionInfo.SetSinceTxId(version.GetTxId());
    *versionInfo.MutableSchema() = schema;

    auto it = ActualSchemaForPreset.find(presetId);
    if (it == ActualSchemaForPreset.end()) {
        ActualSchemaForPreset.emplace(presetId, schema);
    } else {
        *versionInfo.MutableDiff() = NOlap::TSchemaDiffView::MakeSchemasDiff(it->second, schema);
        it->second = schema;
    }

    Schema::SaveSchemaPresetVersionInfo(db, presetId, version, versionInfo);
    if (!PrimaryIndex) {
        PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(
            TabletId, DataAccessorsManager, StoragesManager, version, NOlap::IColumnEngine::TSchemaInitializationData(versionInfo));
        for (auto&& i : Tables) {
            PrimaryIndex->RegisterTable(i.first);
        }
        if (manager->IsReady()) {
            PrimaryIndex->OnTieringModified(manager, Ttl, {});
        }
    } else {
        PrimaryIndex->RegisterSchemaVersion(version, NOlap::IColumnEngine::TSchemaInitializationData(versionInfo));
    }
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TTablesManager::CreateAddShardingInfoTx(
    TColumnShard& owner, const ui64 pathId, const ui64 versionId, const NSharding::TGranuleShardingLogicContainer& tabletShardingLogic) const {
    return std::make_unique<TTxAddShardingInfo>(owner, tabletShardingLogic, pathId, versionId);
}

void TTablesManager::AddTableVersion(const ui64 pathId, const NOlap::TSnapshot& version,
    const NKikimrTxColumnShard::TTableVersionInfo& versionInfo, const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema,
    NIceDb::TNiceDb& db, std::shared_ptr<TTiersManager>& manager) {
    auto it = Tables.find(pathId);
    AFL_VERIFY(it != Tables.end());
    auto& table = it->second;

    bool isTtlModified = false;
    if (versionInfo.HasTtlSettings()) {
        isTtlModified = true;
        const auto& ttlSettings = versionInfo.GetTtlSettings();
        if (ttlSettings.HasEnabled()) {
            Ttl.SetPathTtl(pathId, TTtl::TDescription(ttlSettings.GetEnabled()));
        } else {
            Ttl.DropPathTtl(pathId);
        }
    }

    if (versionInfo.HasSchemaPresetId()) {
        AFL_VERIFY(!schema);
        Y_ABORT_UNLESS(SchemaPresetsIds.contains(versionInfo.GetSchemaPresetId()));
    } else if (schema) {
        TSchemaPreset fakePreset;
        if (SchemaPresetsIds.empty()) {
            Y_ABORT_UNLESS(RegisterSchemaPreset(fakePreset, db));
        } else {
            Y_ABORT_UNLESS(SchemaPresetsIds.contains(fakePreset.GetId()));
        }
        AddSchemaVersion(fakePreset.GetId(), version, *schema, db, manager);
    }

    if (isTtlModified) {
        if (PrimaryIndex && manager->IsReady()) {
            PrimaryIndex->OnTieringModified(manager, Ttl, pathId);
        }
    }
    Schema::SaveTableVersionInfo(db, pathId, version, versionInfo);
    table.AddVersion(version);
}

TTablesManager::TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
    const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager, const ui64 tabletId)
    : StoragesManager(storagesManager)
    , DataAccessorsManager(dataAccessorsManager)
    , LoadTimeCounters(std::make_unique<TTableLoadTimeCounters>())
    , TabletId(tabletId) {
}

bool TTablesManager::TryFinalizeDropPathOnExecute(NTable::TDatabase& dbTable, const ui64 pathId) const {
    auto itDrop = PathsToDrop.find(pathId);
    AFL_VERIFY(itDrop != PathsToDrop.end());
    AFL_VERIFY(!GetPrimaryIndexSafe().HasDataInPathId(pathId));
    NIceDb::TNiceDb db(dbTable);
    NColumnShard::Schema::EraseTableInfo(db, pathId);
    const auto& itTable = Tables.find(pathId);
    AFL_VERIFY(itTable != Tables.end())("problem", "No schema for path")("path_id", pathId);
    for (auto&& tableVersion : itTable->second.GetVersions()) {
        NColumnShard::Schema::EraseTableVersionInfo(db, pathId, tableVersion);
    }
    return true;
}

bool TTablesManager::TryFinalizeDropPathOnComplete(const ui64 pathId) {
    auto itDrop = PathsToDrop.find(pathId);
    AFL_VERIFY(itDrop != PathsToDrop.end());
    AFL_VERIFY(!GetPrimaryIndexSafe().HasDataInPathId(pathId));
    AFL_VERIFY(MutablePrimaryIndex().ErasePathId(pathId));
    PathsToDrop.erase(itDrop);
    const auto& itTable = Tables.find(pathId);
    AFL_VERIFY(itTable != Tables.end())("problem", "No schema for path")("path_id", pathId);
    Tables.erase(itTable);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("method", "TryFinalizeDropPathOnComplete")("path_id", pathId)("size", Tables.size());
    return true;
}

}   // namespace NKikimr::NColumnShard
