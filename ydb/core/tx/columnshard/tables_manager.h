#pragma once

#include "columnshard_schema.h"

#include "blobs_action/abstract/storages_manager.h"
#include "data_accessor/manager.h"
#include "engines/column_engine.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {


template <class TVersionData>
class TVersionedSchema {
private:
    TMap<NOlap::TSnapshot, ui64> Versions;
    TMap<ui64, TVersionData> VersionsById;
    TMap<ui64, NOlap::TSnapshot> MinVersionById;

public:
    bool IsEmpty() const {
        return VersionsById.empty();
    }

    const TMap<ui64, TVersionData>& GetVersionsById() const {
        return VersionsById;
    }

    TMap<ui64, TVersionData>& MutableVersionsById() {
        return VersionsById;
    }

    NOlap::TSnapshot GetMinVersionForId(const ui64 sVersion) const {
        auto it = MinVersionById.find(sVersion);
        Y_ABORT_UNLESS(it != MinVersionById.end());
        return it->second;
    }

    void AddVersion(const NOlap::TSnapshot& snapshot, const TVersionData& versionInfo) {
        ui64 ssVersion = 0;
        if (versionInfo.HasSchema()) {
            ssVersion = versionInfo.GetSchema().GetVersion();
        }
        VersionsById.emplace(ssVersion, versionInfo);
        Y_ABORT_UNLESS(Versions.emplace(snapshot, ssVersion).second);

        auto it = MinVersionById.find(ssVersion);
        if (it == MinVersionById.end()) {
            MinVersionById.emplace(ssVersion, snapshot);
        } else {
            it->second = std::min(snapshot, it->second);
        }
    }
};

class TSchemaPreset: public TVersionedSchema<NKikimrTxColumnShard::TSchemaPresetVersionInfo> {
public:
    using TSchemaPresetVersionInfo = NKikimrTxColumnShard::TSchemaPresetVersionInfo;
    ui32 Id = 0;
    TString Name;

public:
    bool IsStandaloneTable() const {
        return Id == 0;
    }

    const TString& GetName() const {
        return Name;
    }

    ui32 GetId() const {
        return Id;
    }

    void Deserialize(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto);

    template <class TRow>
    bool InitFromDB(const TRow& rowset) {
        Id = rowset.template GetValue<Schema::SchemaPresetInfo::Id>();
        if (!IsStandaloneTable()) {
            Name = rowset.template GetValue<Schema::SchemaPresetInfo::Name>();
        }
        Y_ABORT_UNLESS(!Id || Name == "default", "Unsupported preset at load time");
        return true;
    }
};

class TTableInfo {
    const TInternalPathId PathId;
    TLocalPathId LocalPathId; //path id that the table is known as at SchemeShard
    std::optional<NOlap::TSnapshot> DropVersion;
    YDB_READONLY_DEF(TSet<NOlap::TSnapshot>, Versions);

public:
    bool IsEmpty() const {
        return Versions.empty();
    }

    TInternalPathId GetPathId() const {
        return PathId;
    }

    TLocalPathId GetLocalPathId() const {
        return LocalPathId;
    }

    const NOlap::TSnapshot& GetDropVersionVerified() const {
        AFL_VERIFY(DropVersion);
        return *DropVersion;
    }

    void SetDropVersion(const NOlap::TSnapshot& version) {
        AFL_VERIFY(!DropVersion)("exists", DropVersion->DebugString())("version", version.DebugString());
        DropVersion = version;
    }

    void AddVersion(const NOlap::TSnapshot& snapshot) {
        Versions.insert(snapshot);
    }

    void UpdateLocalPathIdOnExecute(NIceDb::TNiceDb& db, const TLocalPathId newLocalPathId) {
        Schema::UpdateTableLocalPathId(db, PathId, newLocalPathId);
    }

    void UpdateLocalPathIdOnComplete(const TLocalPathId newLocalPathId) {
        LocalPathId = newLocalPathId;
    }

    bool IsDropped(const std::optional<NOlap::TSnapshot>& minReadSnapshot = std::nullopt) const {
        if (!DropVersion) {
            return false;
        }
        if (!minReadSnapshot) {
            return true;
        }
        return *DropVersion < *minReadSnapshot;
    }

    TTableInfo(const TInternalPathId pathId, const TLocalPathId localPathId)
        : PathId(pathId)
        , LocalPathId(localPathId)
    {}

    template <class TRow>
    static TTableInfo InitFromDB(const TRow& rowset) {
        const auto pathId = TInternalPathId::FromInternalPathIdValue(rowset.template GetValue<Schema::TableInfo::PathId>());
        const auto localPathId = TLocalPathId::FromLocalPathIdValue(rowset.template HaveValue<Schema::TableInfo::LocalPathId>() ? rowset.template GetValue<Schema::TableInfo::LocalPathId>() : pathId.GetInternalPathIdValue());
        TTableInfo result(pathId, localPathId);
        if (rowset.template HaveValue<Schema::TableInfo::DropStep>() && rowset.template HaveValue<Schema::TableInfo::DropTxId>()) {
            result.DropVersion.emplace(
                rowset.template GetValue<Schema::TableInfo::DropStep>(), rowset.template GetValue<Schema::TableInfo::DropTxId>());
        }
        return result;
    }
};

class TTtlVersions {
private:
    THashMap<TInternalPathId, std::map<NOlap::TSnapshot, std::optional<NOlap::TTiering>>> Ttl;

    void AddVersion(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot, std::optional<NOlap::TTiering> ttl) {
        AFL_VERIFY(Ttl[pathId].emplace(snapshot, ttl).second)("snapshot", snapshot);
    }

public:
    void AddVersionFromProto(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot, const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings) {
        std::optional<NOlap::TTiering> ttlVersion;
        if (ttlSettings.HasEnabled()) {
            NOlap::TTiering deserializedTtl;
            AFL_VERIFY(deserializedTtl.DeserializeFromProto(ttlSettings.GetEnabled()).IsSuccess());
            ttlVersion.emplace(std::move(deserializedTtl));
        }
        AddVersion(pathId, snapshot, ttlVersion);
    }

    std::optional<NOlap::TTiering> GetTableTtl(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        auto findTable = Ttl.FindPtr(pathId);
        if (!findTable) {
            return std::nullopt;
        }
        const auto findTtl = findTable->upper_bound(snapshot);
        if (findTtl == findTable->begin()) {
            return std::nullopt;
        }
        return std::prev(findTtl)->second;
    }

    ui64 GetMemoryUsage() const {
        ui64 memory = 0;
        for (const auto& [_, ttlVersions] : Ttl) {
            memory += ttlVersions.size() * sizeof(NOlap::TTiering);
        }
        return memory;
    }
};

class TTablesManager {
private:
    THashMap<TInternalPathId, TTableInfo> Tables;
    THashMap<TLocalPathId, TInternalPathId> LocalToInternalPathIds;
    THashMap<TLocalPathId, TInternalPathId> RenamingLocalToInternalPathIds; // Paths that are being renamed
    THashSet<ui32> SchemaPresetsIds;
    THashMap<ui32, NKikimrSchemeOp::TColumnTableSchema> ActualSchemaForPreset;
    std::map<NOlap::TSnapshot, THashSet<NColumnShard::TInternalPathId>> PathsToDrop;
    TTtlVersions Ttl;
    std::unique_ptr<NOlap::IColumnEngine> PrimaryIndex;
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::unique_ptr<TTableLoadTimeCounters> LoadTimeCounters;
    std::shared_ptr<NOlap::TSchemaObjectsCache> SchemaObjectsCache;
    std::shared_ptr<TPortionIndexStats> PortionsStats;
    ui64 TabletId = 0;

public:
    friend class TTxInit;

    TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NOlap::TSchemaObjectsCache>& schemaCache, const std::shared_ptr<TPortionIndexStats>& portionsStats,
        const ui64 tabletId);

    const std::unique_ptr<TTableLoadTimeCounters>& GetLoadTimeCounters() const {
        return LoadTimeCounters;
    }

    bool TryFinalizeDropPathOnExecute(NTable::TDatabase& dbTable, const TInternalPathId pathId) const;
    bool TryFinalizeDropPathOnComplete(const NColumnShard::TInternalPathId pathId);

    THashMap<TInternalPathId, NOlap::TTiering> GetTtl(const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        THashMap<TInternalPathId, NOlap::TTiering> ttl;
        for (const auto& [pathId, info] : Tables) {
            if (info.IsDropped(snapshot)) {
                continue;
            }
            if (auto tableTtl = Ttl.GetTableTtl(pathId, snapshot)) {
                ttl.emplace(pathId, std::move(*tableTtl));
            }
        }
        return ttl;
    }

    std::optional<NOlap::TTiering> GetTableTtl(const TInternalPathId pathId, const NOlap::TSnapshot& snapshot = NOlap::TSnapshot::Max()) const {
        return Ttl.GetTableTtl(pathId, snapshot);
    }

    const std::map<NOlap::TSnapshot, THashSet<NColumnShard::TInternalPathId>>& GetPathsToDrop() const {
        return PathsToDrop;
    }

    THashSet<NColumnShard::TInternalPathId> GetPathsToDrop(const NOlap::TSnapshot& minReadSnapshot) const {
        THashSet<NColumnShard::TInternalPathId> result;
        for (auto&& i : PathsToDrop) {
            if (minReadSnapshot < i.first) {
                break;
            }
            result.insert(i.second.begin(), i.second.end());
        }
        return result;
    }

    size_t GetTableCount() const {
        return Tables.size();
    }

    template<typename F> 
    void ForEachPathId(F&& f) const {
        for (const auto& [pathId, _]: Tables) {
            f(pathId);
        }
    }

    const THashSet<ui32>& GetSchemaPresets() const {
        return SchemaPresetsIds;
    }

    bool HasPrimaryIndex() const {
        return !!PrimaryIndex;
    }

    void MoveTableProposeOnExecute(const TLocalPathId localPathId) {
        NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("local_path_id", localPathId);
        const auto* p = LocalToInternalPathIds.FindPtr(localPathId);
        AFL_VERIFY(p);
        AFL_VERIFY(RenamingLocalToInternalPathIds.emplace(localPathId, *p).second)("internal_path_id", *p);
        AFL_VERIFY(LocalToInternalPathIds.erase(localPathId));
    }

    void MoveTableProgressOnExecute(NIceDb::TNiceDb& db, const TLocalPathId oldLocalPathId, const TLocalPathId newLocalPathId) {
        NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("event", "move_table_progress_on_execute")("old_local_path_id", oldLocalPathId)("new_local_path_id", newLocalPathId);
        const auto* pathId = RenamingLocalToInternalPathIds.FindPtr(oldLocalPathId);
        AFL_VERIFY(pathId);
        auto* table = Tables.FindPtr(*pathId);
        AFL_VERIFY(pathId);
        table->UpdateLocalPathIdOnExecute(db, newLocalPathId);
    }

    void MoveTableProgressOnComplete(const TLocalPathId oldLocalPathId, const TLocalPathId newLocalPathId) {
        NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("event", "move_table_progress_on_complete")("old_local_path_id", oldLocalPathId)("new_local_path_id", newLocalPathId);
        const auto* pathId = RenamingLocalToInternalPathIds.FindPtr(oldLocalPathId);
        AFL_VERIFY(pathId);
        auto* table = Tables.FindPtr(*pathId);
        table->UpdateLocalPathIdOnComplete(newLocalPathId);
        AFL_VERIFY(RenamingLocalToInternalPathIds.erase(oldLocalPathId));
        AFL_VERIFY(LocalToInternalPathIds.emplace(newLocalPathId, *pathId).second);
    }


    NOlap::IColumnEngine& MutablePrimaryIndex() {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    const NOlap::TIndexInfo& GetIndexInfo(const NOlap::TSnapshot& version) const {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return PrimaryIndex->GetVersionedIndex().GetSchemaVerified(version)->GetIndexInfo();
    }

    const std::unique_ptr<NOlap::IColumnEngine>& GetPrimaryIndex() const {
        return PrimaryIndex;
    }

    const NOlap::IColumnEngine& GetPrimaryIndexSafe() const {
        Y_ABORT_UNLESS(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    template <class TIndex>
    TIndex& MutablePrimaryIndexAsVerified() {
        AFL_VERIFY(!!PrimaryIndex);
        auto result = dynamic_cast<TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return *result;
    }

    template <class TIndex>
    const TIndex& GetPrimaryIndexAsVerified() const {
        AFL_VERIFY(!!PrimaryIndex);
        auto result = dynamic_cast<const TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return *result;
    }

    template <class TIndex>
    const TIndex* GetPrimaryIndexAsOptional() const {
        if (!PrimaryIndex) {
            return nullptr;
        }
        auto result = dynamic_cast<const TIndex*>(PrimaryIndex.get());
        AFL_VERIFY(result);
        return result;
    }

    bool InitFromDB(NIceDb::TNiceDb& db);

    ui64 GetMemoryUsage() const;
    std::optional<TInternalPathId> ResolveInternalPathId(const TLocalPathId localPathId) const {
        if (const auto* internalPathId = LocalToInternalPathIds.FindPtr(localPathId)) {
            return {*internalPathId};
        } else {
            return std::nullopt;
        }
    }
    bool HasTable(const TInternalPathId internalPathId, const bool withDeleted = false, const std::optional<NOlap::TSnapshot>& minReadSnapshot = {}) const;
    bool IsReadyForStartWrite(const NColumnShard::TInternalPathId pathId, const bool withDeleted) const;
    bool IsReadyForFinishWrite(const NColumnShard::TInternalPathId pathId, const NOlap::TSnapshot& minReadSnapshot) const;
    bool HasPreset(const ui32 presetId) const;

    void DropTable(const NColumnShard::TInternalPathId pathId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);
    void DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);

    void RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db);
    bool RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db);

    void AddSchemaVersion(
        const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db);
    void AddTableVersion(const NColumnShard::TInternalPathId pathId, const NOlap::TSnapshot& version, const NKikimrTxColumnShard::TTableVersionInfo& versionInfo,
        const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema, NIceDb::TNiceDb& db);
    bool FillMonitoringReport(NTabletFlatExecutor::TTransactionContext& txc, NJson::TJsonValue& json);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateAddShardingInfoTx(TColumnShard& owner, const NColumnShard::TInternalPathId pathId,
        const ui64 versionId, const NSharding::TGranuleShardingLogicContainer& tabletShardingLogic) const;
};

}   // namespace NKikimr::NColumnShard
