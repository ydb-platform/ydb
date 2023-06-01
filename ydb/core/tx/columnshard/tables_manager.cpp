#include "tables_manager.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"
#include "engines/column_engine_logs.h"
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/tiering/manager.h>


namespace NKikimr::NColumnShard {

void TSchemaPreset::Deserialize(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) {
    Id = presetProto.GetId();
    Name = presetProto.GetName();
}

bool TTablesManager::InitFromDB(NIceDb::TNiceDb& db, const ui64 tabletId) {
    TabletId = tabletId;
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

        THashMap<ui64, TRowVersion> lastVersion;
        while (!rowset.EndOfSet()) {
            const ui64 pathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
            Y_VERIFY(Tables.contains(pathId));
            TRowVersion version(
                rowset.GetValue<Schema::TableVersionInfo::SinceStep>(),
                rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());

            auto& table = Tables.at(pathId);
            TTableInfo::TTableVersionInfo versionInfo;
            Y_VERIFY(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
            Y_VERIFY(SchemaPresets.contains(versionInfo.GetSchemaPresetId()));

            if (!table.IsDropped()) {
                auto& ttlSettings = versionInfo.GetTtlSettings();
                if (ttlSettings.HasEnabled()) {
                    if (!lastVersion.contains(pathId) || lastVersion[pathId] < version) {
                        TTtl::TDescription description(ttlSettings.GetEnabled());
                        Ttl.SetPathTtl(pathId, std::move(description));
                        lastVersion[pathId] = version;
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
            Y_VERIFY(SchemaPresets.contains(id));
            auto& preset = SchemaPresets.at(id);
            TRowVersion version(
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(),
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());

            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_VERIFY(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            preset.AddVersion(version, info);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    for (const auto& [id, preset] : SchemaPresets) {
        if (isFakePresetOnly) {
            Y_VERIFY(id == 0);
        } else {
            Y_VERIFY(id > 0);
        }
        for (const auto& [version, schemaInfo] : preset.GetVersions()) {
            if (schemaInfo.HasSchema()) {
                IndexSchemaVersion(version, schemaInfo.GetSchema());
            }
        }
    }
    return true;
}

bool TTablesManager::LoadIndex(NOlap::TDbWrapper& idxDB, THashSet<NOlap::TUnifiedBlobId>& lostEvictions) {
    if (PrimaryIndex) {
        if (!PrimaryIndex->Load(idxDB, lostEvictions, PathsToDrop)) {
            return false;
        }
    }
    return true;
}

void TTablesManager::Clear() {
    Tables.clear();
    SchemaPresets.clear();
    PathsToDrop.clear();
}

bool TTablesManager::HasTable(const ui64 pathId) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end() || it->second.IsDropped()) {
        return false;
    }
    return true;
}

bool TTablesManager::IsWritableTable(const ui64 pathId) const {
    return HasTable(pathId);
}

bool TTablesManager::HasPreset(const ui32 presetId) const {
    return SchemaPresets.contains(presetId);
}

const TTableInfo& TTablesManager::GetTable(const ui64 pathId) const {
    Y_VERIFY(HasTable(pathId));
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

void TTablesManager::OnTtlUpdate() {
    Ttl.Repeat();
}

void TTablesManager::DropTable(const ui64 pathId, const TRowVersion& version, NIceDb::TNiceDb& db) {
    auto& table = Tables.at(pathId);
    table.SetDropVersion(version);
    PathsToDrop.insert(pathId);
    Ttl.DropPathTtl(pathId);
    Schema::SaveTableDropVersion(db, pathId, version.Step, version.TxId);
}

void TTablesManager::DropPreset(const ui32 presetId, const TRowVersion& version, NIceDb::TNiceDb& db) {
    auto& preset = SchemaPresets.at(presetId);
    Y_VERIFY(preset.GetName() != "default", "Cannot drop the default preset");
    preset.SetDropVersion(version);
    Schema::SaveSchemaPresetDropVersion(db, presetId, version);
}

void TTablesManager::RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db) {
    Y_VERIFY(!HasTable(table.GetPathId()));
    Y_VERIFY(table.IsEmpty());

    Schema::SaveTableInfo(db, table.GetPathId(), table.GetTieringUsage());
    Tables.insert_or_assign(table.GetPathId(), std::move(table));
}

bool TTablesManager::RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db) {
    if (SchemaPresets.contains(schemaPreset.GetId())) {
        return false;
    }
    Schema::SaveSchemaPresetInfo(db, schemaPreset.GetId(), schemaPreset.GetName());
    SchemaPresets.insert_or_assign(schemaPreset.GetId(), schemaPreset);
    return true;
}

void TTablesManager::AddPresetVersion(const ui32 presetId, const TRowVersion& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db) {
    Y_VERIFY(SchemaPresets.contains(presetId));
    auto preset = SchemaPresets.at(presetId);

    TSchemaPreset::TSchemaPresetVersionInfo versionInfo;
    versionInfo.SetId(presetId);
    versionInfo.SetSinceStep(version.Step);
    versionInfo.SetSinceTxId(version.TxId);
    *versionInfo.MutableSchema() = schema;

    auto& schemaPreset = SchemaPresets.at(presetId);
    Schema::SaveSchemaPresetVersionInfo(db, presetId, version, versionInfo);
    schemaPreset.AddVersion(version, versionInfo);
    if (versionInfo.HasSchema()){
        IndexSchemaVersion(version, versionInfo.GetSchema());
    }
}

void TTablesManager::AddTableVersion(const ui64 pathId, const TRowVersion& version, const TTableInfo::TTableVersionInfo& versionInfo, NIceDb::TNiceDb& db) {
    auto& table = Tables.at(pathId);

    if (versionInfo.HasSchemaPresetId()) {
        Y_VERIFY(SchemaPresets.contains(versionInfo.GetSchemaPresetId()));
    } else if (versionInfo.HasSchema()) {
        TSchemaPreset fakePreset;
        if (SchemaPresets.empty()) {
            TSchemaPreset fakePreset;
            Y_VERIFY(RegisterSchemaPreset(fakePreset, db));
            AddPresetVersion(fakePreset.GetId(), version, versionInfo.GetSchema(), db);
        } else {
            Y_VERIFY(SchemaPresets.contains(fakePreset.GetId()));
            AddPresetVersion(fakePreset.GetId(), version, versionInfo.GetSchema(), db);
        }
    }

    if (versionInfo.HasTtlSettings()) {
        const auto& ttlSettings = versionInfo.GetTtlSettings();
        if (ttlSettings.HasEnabled()) {
            Ttl.SetPathTtl(pathId, TTtl::TDescription(ttlSettings.GetEnabled()));
        } else {
            Ttl.DropPathTtl(pathId);
        }
    }
    Schema::SaveTableVersionInfo(db, pathId, version, versionInfo);
    table.AddVersion(version, versionInfo);
}

void TTablesManager::IndexSchemaVersion(const TRowVersion& version, const NKikimrSchemeOp::TColumnTableSchema& schema) {
    NOlap::TSnapshot snapshot{version.Step, version.TxId};
    NOlap::TIndexInfo indexInfo = DeserializeIndexInfoFromProto(schema);
    indexInfo.SetAllKeys();
    if (!PrimaryIndex) {
        PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(TabletId);
    } else {
        Y_VERIFY(PrimaryIndex->GetIndexInfo().GetReplaceKey()->Equals(indexInfo.GetReplaceKey()));
        Y_VERIFY(PrimaryIndex->GetIndexInfo().GetIndexKey()->Equals(indexInfo.GetIndexKey()));
    }
    PrimaryIndex->UpdateDefaultSchema(snapshot, std::move(indexInfo));

    for (auto& columnName : Ttl.TtlColumns()) {
        PrimaryIndex->GetIndexInfo().CheckTtlColumn(columnName);
    }
}

std::shared_ptr<NOlap::TColumnEngineChanges> TTablesManager::StartIndexCleanup(const NOlap::TSnapshot& snapshot, const NOlap::TCompactionLimits& limits, ui32 maxRecords) {
    Y_VERIFY(PrimaryIndex);
    return PrimaryIndex->StartCleanup(snapshot, limits, PathsToDrop, maxRecords);
}

NOlap::TIndexInfo TTablesManager::DeserializeIndexInfoFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    bool forceCompositeMarks = AppData()->FeatureFlags.GetForceColumnTablesCompositeMarks();
    std::optional<NOlap::TIndexInfo> indexInfo = NOlap::TIndexInfo::BuildFromProto(schema, forceCompositeMarks);
    Y_VERIFY(indexInfo);
    return *indexInfo;
}
}
