#include "tables_manager.h"
#include "columnshard_schema.h"
#include "engines/column_engine_logs.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>


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
    json.InsertValue("presets_count", SchemaPresets.size());
    json.InsertValue("to_drop_count", PathsToDrop.size());
    return true;
}

bool TTablesManager::InitFromDB(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<Schema::TableInfo>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            TTableInfo table;
            if (!table.InitFromDB(rowset)) {
                return false;
            }
            if (table.IsDropped()) {
                PathsToDrop.insert(table.GetPathId());
            }
            Tables.insert_or_assign(table.GetPathId(), std::move(table));

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    bool isFakePresetOnly = true;
    {
        auto rowset = db.Table<Schema::SchemaPresetInfo>().Select();
        if (!rowset.IsReady()) {
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
            SchemaPresets.insert_or_assign(preset.GetId(), preset);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::TableVersionInfo>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        THashMap<ui64, NOlap::TSnapshot> lastVersion;
        while (!rowset.EndOfSet()) {
            const ui64 pathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
            Y_ABORT_UNLESS(Tables.contains(pathId));
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::TableVersionInfo::SinceStep>(),
                    rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());

            auto& table = Tables.at(pathId);
            TTableInfo::TTableVersionInfo versionInfo;
            Y_ABORT_UNLESS(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "load_table_version")("path_id", pathId)("snapshot", version)("version", versionInfo.HasSchema() ? versionInfo.GetSchema().GetVersion() : -1);
            Y_ABORT_UNLESS(SchemaPresets.contains(versionInfo.GetSchemaPresetId()));

            if (!table.IsDropped()) {
                auto& ttlSettings = versionInfo.GetTtlSettings();
                if (ttlSettings.HasEnabled()) {
                    auto vIt = lastVersion.find(pathId);
                    if (vIt == lastVersion.end() || vIt->second < version) {
                        TTtl::TDescription description(ttlSettings.GetEnabled());
                        Ttl.SetPathTtl(pathId, std::move(description));
                        lastVersion.emplace(pathId, version);
                    }
                }
            }
            table.AddVersion(version, versionInfo);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
            Y_ABORT_UNLESS(SchemaPresets.contains(id));
            auto& preset = SchemaPresets.at(id);
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(),
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());

            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "load_preset")("preset_id", id)("snapshot", version)("version", info.HasSchema() ? info.GetSchema().GetVersion() : -1);
            preset.AddVersion(version, info);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    for (const auto& [id, preset] : SchemaPresets) {
        if (isFakePresetOnly) {
            Y_ABORT_UNLESS(id == 0);
        } else {
            Y_ABORT_UNLESS(id > 0);
        }
        for (const auto& [version, schemaInfo] : preset.GetVersions()) {
            if (schemaInfo.HasSchema()) {
                AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "index_schema")("preset_id", id)("snapshot", version)("version", schemaInfo.GetSchema().GetVersion());
                IndexSchemaVersion(version, schemaInfo.GetSchema());
            }
        }
    }
    return true;
}

bool TTablesManager::LoadIndex(NOlap::TDbWrapper& idxDB) {
    if (PrimaryIndex) {
        if (!PrimaryIndex->Load(idxDB)) {
            return false;
        }
    }
    return true;
}

bool TTablesManager::HasTable(const ui64 pathId) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end() || it->second.IsDropped()) {
        return false;
    }
    return true;
}

bool TTablesManager::IsReadyForWrite(const ui64 pathId) const {
    return HasPrimaryIndex() && HasTable(pathId);
}

bool TTablesManager::HasPreset(const ui32 presetId) const {
    return SchemaPresets.contains(presetId);
}

const TTableInfo& TTablesManager::GetTable(const ui64 pathId) const {
    Y_ABORT_UNLESS(HasTable(pathId));
    return Tables.at(pathId);
}

ui64 TTablesManager::GetMemoryUsage() const {
    ui64 memory =
        Tables.size() * sizeof(TTableInfo) +
        PathsToDrop.size() * sizeof(ui64) +
        Ttl.PathsCount() * sizeof(TTtl::TDescription) +
        SchemaPresets.size() * sizeof(TSchemaPreset);
    if (PrimaryIndex) {
        memory += PrimaryIndex->MemoryUsage();
    }
    return memory;
}

void TTablesManager::DropTable(const ui64 pathId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    auto& table = Tables.at(pathId);
    table.SetDropVersion(version);
    PathsToDrop.insert(pathId);
    Ttl.DropPathTtl(pathId);
    if (PrimaryIndex) {
        PrimaryIndex->OnTieringModified(nullptr, Ttl);
    }
    Schema::SaveTableDropVersion(db, pathId, version.GetPlanStep(), version.GetTxId());
}

void TTablesManager::DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    auto& preset = SchemaPresets.at(presetId);
    Y_ABORT_UNLESS(preset.GetName() != "default", "Cannot drop the default preset");
    preset.SetDropVersion(version);
    Schema::SaveSchemaPresetDropVersion(db, presetId, version);
}

void TTablesManager::RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db) {
    Y_ABORT_UNLESS(!HasTable(table.GetPathId()));
    Y_ABORT_UNLESS(table.IsEmpty());

    Schema::SaveTableInfo(db, table.GetPathId(), table.GetTieringUsage());
    const ui64 pathId = table.GetPathId();
    Tables.insert_or_assign(pathId, std::move(table));
    if (PrimaryIndex) {
        PrimaryIndex->RegisterTable(pathId);
    }
}

bool TTablesManager::RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db) {
    if (SchemaPresets.contains(schemaPreset.GetId())) {
        return false;
    }
    Schema::SaveSchemaPresetInfo(db, schemaPreset.GetId(), schemaPreset.GetName());
    SchemaPresets.insert_or_assign(schemaPreset.GetId(), schemaPreset);
    return true;
}

void TTablesManager::AddSchemaVersion(const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db) {
    Y_ABORT_UNLESS(SchemaPresets.contains(presetId));
    auto preset = SchemaPresets.at(presetId);

    TSchemaPreset::TSchemaPresetVersionInfo versionInfo;
    versionInfo.SetId(presetId);
    versionInfo.SetSinceStep(version.GetPlanStep());
    versionInfo.SetSinceTxId(version.GetTxId());
    *versionInfo.MutableSchema() = schema;

    auto& schemaPreset = SchemaPresets.at(presetId);
    Schema::SaveSchemaPresetVersionInfo(db, presetId, version, versionInfo);
    schemaPreset.AddVersion(version, versionInfo);
    if (versionInfo.HasSchema()){
        IndexSchemaVersion(version, versionInfo.GetSchema());
        for (auto& columnName : Ttl.TtlColumns()) {
            PrimaryIndex->GetVersionedIndex().GetLastSchema()->GetIndexInfo().CheckTtlColumn(columnName);
        }
    }
}

void TTablesManager::AddTableVersion(const ui64 pathId, const NOlap::TSnapshot& version, const TTableInfo::TTableVersionInfo& versionInfo, NIceDb::TNiceDb& db) {
    auto it = Tables.find(pathId);
    AFL_VERIFY(it != Tables.end());
    auto& table = it->second;

    if (versionInfo.HasSchemaPresetId()) {
        Y_ABORT_UNLESS(SchemaPresets.contains(versionInfo.GetSchemaPresetId()));
    } else if (versionInfo.HasSchema()) {
        TSchemaPreset fakePreset;
        if (SchemaPresets.empty()) {
            TSchemaPreset fakePreset;
            Y_ABORT_UNLESS(RegisterSchemaPreset(fakePreset, db));
            AddSchemaVersion(fakePreset.GetId(), version, versionInfo.GetSchema(), db);
        } else {
            Y_ABORT_UNLESS(SchemaPresets.contains(fakePreset.GetId()));
            AddSchemaVersion(fakePreset.GetId(), version, versionInfo.GetSchema(), db);
        }
    }

    if (versionInfo.HasTtlSettings()) {
        const auto& ttlSettings = versionInfo.GetTtlSettings();
        if (ttlSettings.HasEnabled()) {
            Ttl.SetPathTtl(pathId, TTtl::TDescription(ttlSettings.GetEnabled()));
        } else {
            Ttl.DropPathTtl(pathId);
        }
        if (PrimaryIndex) {
            PrimaryIndex->OnTieringModified(nullptr, Ttl);
        }
    }
    Schema::SaveTableVersionInfo(db, pathId, version, versionInfo);
    table.AddVersion(version, versionInfo);
}

void TTablesManager::IndexSchemaVersion(const NOlap::TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema) {
    NOlap::TIndexInfo indexInfo = DeserializeIndexInfoFromProto(schema);
    indexInfo.SetAllKeys();
    const bool isFirstPrimaryIndexInitialization = !PrimaryIndex;
    if (!PrimaryIndex) {
        PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(TabletId, NOlap::TCompactionLimits(), StoragesManager);
    }
    PrimaryIndex->RegisterSchemaVersion(snapshot, std::move(indexInfo));
    if (isFirstPrimaryIndexInitialization) {
        for (auto&& i : Tables) {
            PrimaryIndex->RegisterTable(i.first);
        }
    }
    PrimaryIndex->OnTieringModified(nullptr, Ttl);
}

NOlap::TIndexInfo TTablesManager::DeserializeIndexInfoFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    std::optional<NOlap::TIndexInfo> indexInfo = NOlap::TIndexInfo::BuildFromProto(schema);
    Y_ABORT_UNLESS(indexInfo);
    return *indexInfo;
}

TTablesManager::TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const ui64 tabletId)
    : StoragesManager(storagesManager)
    , TabletId(tabletId)
{
}

bool TTablesManager::TryFinalizeDropPath(NTable::TDatabase& dbTable, const ui64 pathId) {
    auto itDrop = PathsToDrop.find(pathId);
    if (itDrop == PathsToDrop.end()) {
        return false;
    }
    if (GetPrimaryIndexSafe().HasDataInPathId(pathId)) {
        return false;
    }
    PathsToDrop.erase(itDrop);
    NIceDb::TNiceDb db(dbTable);
    NColumnShard::Schema::EraseTableInfo(db, pathId);
    const auto& table = Tables.find(pathId);
    Y_ABORT_UNLESS(table != Tables.end(), "No schema for path %lu", pathId);
    for (auto&& tableVersion : table->second.GetVersions()) {
        NColumnShard::Schema::EraseTableVersionInfo(db, pathId, tableVersion.first);
    }
    return true;
}

}
