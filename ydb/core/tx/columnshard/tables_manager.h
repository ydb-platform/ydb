#pragma once

#include "columnshard_schema.h"

#include "blobs_action/abstract/storages_manager.h"
#include "data_accessor/manager.h"
#include "engines/column_engine.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>

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
public:
    ui64 PathId;
    std::optional<NOlap::TSnapshot> DropVersion;
    YDB_READONLY_DEF(TSet<NOlap::TSnapshot>, Versions);

public:
    bool IsEmpty() const {
        return Versions.empty();
    }

    ui64 GetPathId() const {
        return PathId;
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

    bool IsDropped(const std::optional<NOlap::TSnapshot>& minReadSnapshot = std::nullopt) const {
        if (!DropVersion) {
            return false;
        }
        if (!minReadSnapshot) {
            return true;
        }
        return *DropVersion < *minReadSnapshot;
    }

    TTableInfo() = default;

    TTableInfo(const ui64 pathId)
        : PathId(pathId) {
    }

    template <class TRow>
    bool InitFromDB(const TRow& rowset) {
        PathId = rowset.template GetValue<Schema::TableInfo::PathId>();
        if (rowset.template HaveValue<Schema::TableInfo::DropStep>() && rowset.template HaveValue<Schema::TableInfo::DropTxId>()) {
            DropVersion.emplace(
                rowset.template GetValue<Schema::TableInfo::DropStep>(), rowset.template GetValue<Schema::TableInfo::DropTxId>());
        }
        return true;
    }
};

class TTablesManager {
private:
    THashMap<ui64, TTableInfo> Tables;
    THashSet<ui32> SchemaPresetsIds;
    THashMap<ui32, NKikimrSchemeOp::TColumnTableSchema> ActualSchemaForPreset;
    std::map<NOlap::TSnapshot, THashSet<ui64>> PathsToDrop;
    THashMap<ui64, NOlap::TTiering> Ttl;
    std::unique_ptr<NOlap::IColumnEngine> PrimaryIndex;
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::unique_ptr<TTableLoadTimeCounters> LoadTimeCounters;
    std::shared_ptr<NOlap::TSchemaObjectsCache> SchemaObjectsCache;
    ui64 TabletId = 0;

public:
    friend class TTxInit;

    TTablesManager(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NOlap::TSchemaObjectsCache>& schemaCache, const ui64 tabletId);

    const std::unique_ptr<TTableLoadTimeCounters>& GetLoadTimeCounters() const {
        return LoadTimeCounters;
    }

    bool TryFinalizeDropPathOnExecute(NTable::TDatabase& dbTable, const ui64 pathId) const;
    bool TryFinalizeDropPathOnComplete(const ui64 pathId);

    const THashMap<ui64, NOlap::TTiering>& GetTtl() const {
        return Ttl;
    }

    const std::map<NOlap::TSnapshot, THashSet<ui64>>& GetPathsToDrop() const {
        return PathsToDrop;
    }

    THashSet<ui64> GetPathsToDrop(const NOlap::TSnapshot& minReadSnapshot) const {
        THashSet<ui64> result;
        for (auto&& i : PathsToDrop) {
            if (minReadSnapshot < i.first) {
                break;
            }
            result.insert(i.second.begin(), i.second.end());
        }
        return result;
    }

    const THashMap<ui64, TTableInfo>& GetTables() const {
        return Tables;
    }

    const THashSet<ui32>& GetSchemaPresets() const {
        return SchemaPresetsIds;
    }

    bool HasPrimaryIndex() const {
        return !!PrimaryIndex;
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

    const TTableInfo& GetTable(const ui64 pathId) const;
    ui64 GetMemoryUsage() const;

    bool HasTable(const ui64 pathId, const bool withDeleted = false, const std::optional<NOlap::TSnapshot> minReadSnapshot = std::nullopt) const;
    bool IsReadyForStartWrite(const ui64 pathId, const bool withDeleted) const;
    bool IsReadyForFinishWrite(const ui64 pathId, const NOlap::TSnapshot& minReadSnapshot) const;
    bool HasPreset(const ui32 presetId) const;

    void DropTable(const ui64 pathId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);
    void DropPreset(const ui32 presetId, const NOlap::TSnapshot& version, NIceDb::TNiceDb& db);

    void RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db);
    bool RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db);

    void AddSchemaVersion(
        const ui32 presetId, const NOlap::TSnapshot& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db);
    void AddTableVersion(const ui64 pathId, const NOlap::TSnapshot& version, const NKikimrTxColumnShard::TTableVersionInfo& versionInfo,
        const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema, NIceDb::TNiceDb& db);
    bool FillMonitoringReport(NTabletFlatExecutor::TTransactionContext& txc, NJson::TJsonValue& json);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> CreateAddShardingInfoTx(TColumnShard& owner, const ui64 pathId,
        const ui64 versionId, const NSharding::TGranuleShardingLogicContainer& tabletShardingLogic) const;
};

}   // namespace NKikimr::NColumnShard
