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

namespace {

TInternalPathId GetInitialMaxInternalPathId(const ui64 tabletId) {
    static constexpr ui64 InternalPathIdBase = 1'000'000'000; //Use a value presumably greater than any really used
    static constexpr ui64 InternalPathIdTabletMod = 1'000'000; //Use different start value for tablets
    return TInternalPathId::FromRawValue(InternalPathIdBase + tabletId * InternalPathIdTabletMod);
}

} //namespace

std::optional<NColumnShard::TSchemeShardLocalPathId> TTablesManager::ResolveSchemeShardLocalPathId(const TInternalPathId internalPathId) const {
    if (!HasTable(internalPathId)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("method", "resolve_ss_path_id")("internal", internalPathId)("result", "not_found");
        return std::nullopt;
    }
    const auto p = Tables.FindPtr(internalPathId);
    AFL_VERIFY(p)("internal", internalPathId);
    return p->GetPathId().SchemeShardLocalPathId;
}

std::optional<TInternalPathId> TTablesManager::ResolveInternalPathId(const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId) const {
    if (const auto* internalPathId = SchemeShardLocalToInternal.FindPtr(schemeShardLocalPathId)) {
        return {*internalPathId};
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("method", "resolve_internal_path_id")("ss_local", schemeShardLocalPathId)("result", "not_found");
        return std::nullopt;
    }
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
    {
        std::optional<ui64> maxPathId;
        if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::MaxInternalPathId, maxPathId)) {
            return false;
        }
        if (maxPathId) {
            MaxInternalPathId =TInternalPathId::FromRawValue(*maxPathId);
            AFL_VERIFY(MaxInternalPathId >= GetInitialMaxInternalPathId(TabletId));
        }
    }
    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->TableLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::Tables");
        auto rowset = db.Table<Schema::TableInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        while (!rowset.EndOfSet()) {
            TTableInfo table = table.InitFromDB(rowset);
            if (table.IsDropped()) {
                AFL_VERIFY(PathsToDrop[table.GetDropVersionVerified()].emplace(table.GetPathId().InternalPathId).second);
            }
            const auto& pathId = table.GetPathId();
            AFL_VERIFY(pathId.InternalPathId <= MaxInternalPathId)("path_id", pathId)("max_internal_path_id", MaxInternalPathId);
            AFL_VERIFY(Tables.emplace(pathId.InternalPathId, std::move(table)).second);
            AFL_VERIFY(SchemeShardLocalToInternal.emplace(pathId.SchemeShardLocalPathId, pathId.InternalPathId).second);

            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    std::optional<TSchemaPreset> preset;
    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->SchemaPresetLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::SchemaPresets");
        auto rowset = db.Table<Schema::SchemaPresetInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        if (!rowset.EndOfSet()) {
            preset = TSchemaPreset();
            preset->InitFromDB(rowset);

            if (preset->IsStandaloneTable()) {
                Y_VERIFY_S(!preset->GetName(), "Preset name: " + preset->GetName());
                AFL_VERIFY(!preset->Id);
            } else {
                Y_VERIFY_S(preset->GetName() == "default", "Preset name: " + preset->GetName());
                AFL_VERIFY(preset->Id);
            }
            AFL_VERIFY(SchemaPresetsIds.emplace(preset->GetId()).second);
            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }

        AFL_VERIFY(rowset.EndOfSet())("reson", "multiple_presets_not_supported");
    }

    {
        TLoadTimeSignals::TLoadTimer timer = LoadTimeCounters->TableVersionsLoadTimeCounters.StartGuard();
        TMemoryProfileGuard g("TTablesManager/InitFromDB::Versions");
        auto rowset = db.Table<Schema::TableVersionInfo>().Select();
        if (!rowset.IsReady()) {
            timer.AddLoadingFail();
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto pathId = TInternalPathId::FromRawValue(rowset.GetValue<Schema::TableVersionInfo::PathId>());
            const auto table = Tables.FindPtr(pathId);
            AFL_VERIFY(table);
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::TableVersionInfo::SinceStep>(), rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());

            NKikimrTxColumnShard::TTableVersionInfo versionInfo;
            AFL_VERIFY(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "load_table_version")("path_id", pathId)("snapshot", version);
            AFL_VERIFY(preset);
            AFL_VERIFY(preset->Id == versionInfo.GetSchemaPresetId())("preset", preset->Id)("table", versionInfo.GetSchemaPresetId());

            if (versionInfo.HasTtlSettings()) {
                Ttl.AddVersionFromProto(pathId, version, versionInfo.GetTtlSettings());
            }
            table->AddVersion(version);
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
            AFL_VERIFY(preset);
            AFL_VERIFY(preset->Id == id)("preset", preset->Id)("schema", id);
            NOlap::TSnapshot version(
                rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(), rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());

            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "load_preset")("preset_id", id)("snapshot", version)(
                "version", info.HasSchema() ? info.GetSchema().GetVersion() : -1);

            AFL_VERIFY(info.HasSchema());
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "index_schema")("preset_id", id)("snapshot", version)(
                "version", info.GetSchema().GetVersion());
            NOlap::IColumnEngine::TSchemaInitializationData schemaInitializationData(info);
            if (!PrimaryIndex) {
                PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(TabletId, SchemaObjectsCache.GetObjectPtrVerified(),
                    DataAccessorsManager, StoragesManager, version, preset->Id, schemaInitializationData, PortionsStats);
            } else if (PrimaryIndex->GetVersionedIndex().IsEmpty() ||
                       info.GetSchema().GetVersion() > PrimaryIndex->GetVersionedIndex().GetLastSchema()->GetVersion()) {
                PrimaryIndex->RegisterSchemaVersion(version, preset->Id, schemaInitializationData);
            } else {
                PrimaryIndex->RegisterOldSchemaVersion(version, preset->Id, schemaInitializationData);
            }

            if (!rowset.Next()) {
                timer.AddLoadingFail();
                return false;
            }
        }
    }

    TMemoryProfileGuard g("TTablesManager/InitFromDB::Other");
    for (auto&& i : Tables) {
        PrimaryIndex->RegisterTable(i.first);
    }
    return true;
}


THashMap<TSchemeShardLocalPathId, TInternalPathId> TTablesManager::ResolveInternalPathIds(const TSchemeShardLocalPathId from, const TSchemeShardLocalPathId to) const {
    THashMap<TSchemeShardLocalPathId, TInternalPathId> result;
    for (const auto& [schemeShardLocalPathId, internalPathId]: SchemeShardLocalToInternal) {
        if ((from <= schemeShardLocalPathId) && (schemeShardLocalPathId <= to)) {
            result.emplace(schemeShardLocalPathId, internalPathId);
        }
    }
    return result;
}

bool TTablesManager::HasTable(const TInternalPathId pathId, const bool withDeleted, const std::optional<NOlap::TSnapshot> minReadSnapshot) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end()) {
        return false;
    }
    if (it->second.IsDropped(minReadSnapshot)) {
        return withDeleted;
    }
    return true;
}

TInternalPathId TTablesManager::CreateInternalPathId(const TSchemeShardLocalPathId schemeShardLocalPathId) {
    if (NYDBTest::TControllers::GetColumnShardController()->IsForcedGenerateInternalPathId() ||
        AppData()->ColumnShardConfig.GetGenerateInternalPathId()) {
        const auto result = TInternalPathId::FromRawValue(MaxInternalPathId.GetRawValue() + 1);
        MaxInternalPathId = result;
        return result;
    } else {
        return TInternalPathId::FromRawValue(schemeShardLocalPathId.GetRawValue());
    }
}

bool TTablesManager::IsReadyForStartWrite(const TInternalPathId pathId, const bool withDeleted) const {
    return HasPrimaryIndex() && HasTable(pathId, withDeleted);
}

bool TTablesManager::IsReadyForFinishWrite(const TInternalPathId pathId, const NOlap::TSnapshot& minReadSnapshot) const {
    return HasPrimaryIndex() && HasTable(pathId, false, minReadSnapshot);
}

bool TTablesManager::HasPreset(const ui32 presetId) const {
    return SchemaPresetsIds.contains(presetId);
}

const TTableInfo& TTablesManager::GetTable(const TInternalPathId pathId) const {
    Y_ABORT_UNLESS(HasTable(pathId));
    return Tables.at(pathId);
}

ui64 TTablesManager::GetMemoryUsage() const {
    ui64 memory = Tables.size() * sizeof(TTableInfo) + PathsToDrop.size() * sizeof(ui64) + Ttl.GetMemoryUsage();
    if (PrimaryIndex) {
        memory += PrimaryIndex->MemoryUsage();
    }
    return memory;
}

void TTablesManager::DropTable(const TInternalPathId pathId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    auto* table = Tables.FindPtr(pathId);
    AFL_VERIFY(table);
    table->SetDropVersion(version);
    AFL_VERIFY(PathsToDrop[version].emplace(pathId).second);
    Schema::SaveTableDropVersion(db, pathId, version.GetPlanStep(), version.GetTxId());
}

void TTablesManager::DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db) {
    AFL_VERIFY(SchemaPresetsIds.contains(presetId));
    SchemaPresetsIds.erase(presetId);
    Schema::SaveSchemaPresetDropVersion(db, presetId, version);
}

void TTablesManager::RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db) {
    Y_ABORT_UNLESS(!HasTable(table.GetPathId().InternalPathId));
    Y_ABORT_UNLESS(table.IsEmpty());
    NYDBTest::TControllers::GetColumnShardController()->OnAddPathId(TabletId, table.GetPathId());

    Schema::SaveTableInfo(db, table.GetPathId().InternalPathId);
    const auto pathId = table.GetPathId().InternalPathId;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("method", "RegisterTable")("path_id", pathId);
    AFL_VERIFY(Tables.emplace(pathId, std::move(table)).second)("path_id", pathId)("size", Tables.size());
    AFL_VERIFY(SchemeShardLocalToInternal.emplace(table.GetPathId().SchemeShardLocalPathId, table.GetPathId().InternalPathId).second);
    Schema::SaveTableSchemeShardLocalPathId(db, table.GetPathId().InternalPathId, table.GetPathId().SchemeShardLocalPathId);
    Schema::SaveSpecialValue(db, Schema::EValueIds::MaxInternalPathId, MaxInternalPathId.GetRawValue());
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

void TTablesManager::AddSchemaVersion(
    const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db) {
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

    versionInfo.MutableSchema()->SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
    Schema::SaveSchemaPresetVersionInfo(db, presetId, version, versionInfo);
    if (!PrimaryIndex) {
        PrimaryIndex = std::make_unique<NOlap::TColumnEngineForLogs>(TabletId, SchemaObjectsCache.GetObjectPtrVerified(), DataAccessorsManager,
            StoragesManager, version, presetId, NOlap::IColumnEngine::TSchemaInitializationData(versionInfo), PortionsStats);
        for (auto&& i : Tables) {
            PrimaryIndex->RegisterTable(i.first);
        }
        PrimaryIndex->OnTieringModified(GetTtl());
    } else {
        PrimaryIndex->RegisterSchemaVersion(version, presetId, NOlap::IColumnEngine::TSchemaInitializationData(versionInfo));
    }
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TTablesManager::CreateAddShardingInfoTx(
    TColumnShard& owner, const TSchemeShardLocalPathId schemeShardLocalPathId, const ui64 versionId,
    const NSharding::TGranuleShardingLogicContainer& tabletShardingLogic) const {
    const auto* internalPathId = SchemeShardLocalToInternal.FindPtr(schemeShardLocalPathId);
    AFL_VERIFY(internalPathId)("scheme_shard_local_path_id", schemeShardLocalPathId);
    return std::make_unique<TTxAddShardingInfo>(owner, tabletShardingLogic, *internalPathId, versionId);
}

void TTablesManager::AddTableVersion(const TInternalPathId pathId, const NOlap::TSnapshot& version,
    const NKikimrTxColumnShard::TTableVersionInfo& versionInfo, const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema, NIceDb::TNiceDb& db) {
    auto it = Tables.find(pathId);
    AFL_VERIFY(it != Tables.end());
    auto& table = it->second;

    bool isTtlModified = false;
    if (versionInfo.HasTtlSettings()) {
        isTtlModified = true;
        Ttl.AddVersionFromProto(pathId, version, versionInfo.GetTtlSettings());
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
        AddSchemaVersion(fakePreset.GetId(), version, *schema, db);
    }

    if (isTtlModified) {
        if (PrimaryIndex) {
            PrimaryIndex->OnTieringModified(GetTableTtl(pathId), pathId);
        }
    }
    Schema::SaveTableVersionInfo(db, pathId, version, versionInfo);
    table.AddVersion(version);
}

TTablesManager::TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
    const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const std::shared_ptr<NOlap::TSchemaObjectsCache>& schemaCache, const std::shared_ptr<TPortionIndexStats>& portionsStats,
    const ui64 tabletId)
    : StoragesManager(storagesManager)
    , DataAccessorsManager(dataAccessorsManager)
    , LoadTimeCounters(std::make_unique<TTableLoadTimeCounters>())
    , SchemaObjectsCache(schemaCache)
    , PortionsStats(portionsStats)
    , TabletId(tabletId)
    , MaxInternalPathId(GetInitialMaxInternalPathId(TabletId)) {
}

bool TTablesManager::TryFinalizeDropPathOnExecute(NTable::TDatabase& dbTable, const TInternalPathId pathId) const {
    const auto& itTable = Tables.find(pathId);
    AFL_VERIFY(itTable != Tables.end())("problem", "No schema for path")("path_id", pathId);
    auto itDrop = PathsToDrop.find(itTable->second.GetDropVersionVerified());
    AFL_VERIFY(itDrop != PathsToDrop.end());
    AFL_VERIFY(itDrop->second.contains(pathId));

    AFL_VERIFY(!GetPrimaryIndexSafe().HasDataInPathId(pathId));
    NIceDb::TNiceDb db(dbTable);
    NColumnShard::Schema::EraseTableInfo(db, pathId);
    for (auto&& tableVersion : itTable->second.GetVersions()) {
        NColumnShard::Schema::EraseTableVersionInfo(db, pathId, tableVersion);
    }
    return true;
}

bool TTablesManager::TryFinalizeDropPathOnComplete(const TInternalPathId pathId) {
    const auto& itTable = Tables.find(pathId);
    AFL_VERIFY(itTable != Tables.end())("problem", "No schema for path")("path_id", pathId);
    {
        auto itDrop = PathsToDrop.find(itTable->second.GetDropVersionVerified());
        AFL_VERIFY(itDrop != PathsToDrop.end());
        AFL_VERIFY(itDrop->second.erase(pathId));
        if (itDrop->second.empty()) {
            PathsToDrop.erase(itDrop);
        }
    }
    AFL_VERIFY(!GetPrimaryIndexSafe().HasDataInPathId(pathId));
    AFL_VERIFY(MutablePrimaryIndex().ErasePathId(pathId));
    Tables.erase(itTable);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("method", "TryFinalizeDropPathOnComplete")("path_id", pathId)("size", Tables.size());
    return true;
}

void TTablesManager::MoveTablePropose(const TSchemeShardLocalPathId schemeShardLocalPathId) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("scheme_shard_local_path_id", schemeShardLocalPathId);
    const auto& internalPathId = ResolveInternalPathId(schemeShardLocalPathId);
    AFL_VERIFY(internalPathId);
    AFL_VERIFY(RenamingLocalToInternal.emplace(schemeShardLocalPathId, *internalPathId).second)("internal_path_id", internalPathId);
    AFL_VERIFY(SchemeShardLocalToInternal.erase(schemeShardLocalPathId));
}

void TTablesManager::MoveTableProgress(NIceDb::TNiceDb& db, const TSchemeShardLocalPathId oldSchemeShardLocalPathId, const TSchemeShardLocalPathId newSchemeShardLocalPathId) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)
        ("event", "move_table_progress")("old_path_id", oldSchemeShardLocalPathId)("new_path_id", newSchemeShardLocalPathId);
    AFL_VERIFY(!ResolveInternalPathId(oldSchemeShardLocalPathId));
    AFL_VERIFY(!ResolveInternalPathId(newSchemeShardLocalPathId));
    const auto* internalPathId = RenamingLocalToInternal.FindPtr(oldSchemeShardLocalPathId);
    AFL_VERIFY(internalPathId);
    AFL_VERIFY(HasTable(*internalPathId));
    auto* table = Tables.FindPtr(*internalPathId);
    AFL_VERIFY(table);
    table->UpdateLocalPathId(db, newSchemeShardLocalPathId);
    AFL_VERIFY(RenamingLocalToInternal.erase(oldSchemeShardLocalPathId));
    AFL_VERIFY(SchemeShardLocalToInternal.emplace(newSchemeShardLocalPathId, *internalPathId).second);
    NYDBTest::TControllers::GetColumnShardController()->OnDeletePathId(TabletId, {*internalPathId, oldSchemeShardLocalPathId});
    NYDBTest::TControllers::GetColumnShardController()->OnAddPathId(TabletId, table->GetPathId());
}

}   // namespace NKikimr::NColumnShard
