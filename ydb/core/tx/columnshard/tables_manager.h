#pragma once

#include "columnshard_schema.h"
#include "columnshard_ttl.h"
#include "engines/column_engine.h"

#include "ydb/core/base/row_version.h"
#include "ydb/library/accessor/accessor.h"
#include "ydb/core/protos/tx_columnshard.pb.h"


namespace NKikimr::NColumnShard {

template<class TSchemaProto>
class TVersionedSchema {
protected:
    std::optional<TRowVersion> DropVersion;
    TMap<TRowVersion, TSchemaProto> Versions;

public:
    bool IsDropped() const {
        return DropVersion.has_value();
    }

    bool IsEmpty() const {
        return Versions.empty();
    }

    void SetDropVersion(const TRowVersion& version) {
        DropVersion = version;
    }

    const TMap<TRowVersion, TSchemaProto>& GetVersions() const {
        return Versions;
    }

    const TSchemaProto& GetVersion(const TRowVersion& version) const {
        const TSchemaProto* result = nullptr;
        for (auto ver : Versions) {
            if (ver.first > version) {
                break;
            }
            result = &ver.second;
        }
        Y_VERIFY(!!result);
        return *result;
    }

    void AddVersion(const TRowVersion& version, const TSchemaProto& versionInfo) {
        Versions[version] = versionInfo;
    }
};

class TSchemaPreset : public TVersionedSchema<NKikimrTxColumnShard::TSchemaPresetVersionInfo> {
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
        Y_VERIFY(!Id || Name == "default", "Unsupported preset at load time");

        if (rowset.template HaveValue<Schema::SchemaPresetInfo::DropStep>() &&
            rowset.template HaveValue<Schema::SchemaPresetInfo::DropTxId>())
        {
            DropVersion.emplace();
            DropVersion->Step = rowset.template GetValue<Schema::SchemaPresetInfo::DropStep>();
            DropVersion->TxId = rowset.template GetValue<Schema::SchemaPresetInfo::DropTxId>();
        }
        return true;
    }
};

class TTableInfo : public TVersionedSchema<NKikimrTxColumnShard::TTableVersionInfo> {
public:
    using TTableVersionInfo = NKikimrTxColumnShard::TTableVersionInfo;
    ui64 PathId;
    TString TieringUsage;

public:
    const TString& GetTieringUsage() const {
        return TieringUsage;
    }

    TTableInfo& SetTieringUsage(const TString& data) {
        TieringUsage = data;
        return *this;
    }

    ui64 GetPathId() const {
        return PathId;
    }

    TTableInfo() = default;

    TTableInfo(const ui64 pathId)
        : PathId(pathId)
    {}

    template <class TRow>
    bool InitFromDB(const TRow& rowset) {
        PathId = rowset.template GetValue<Schema::TableInfo::PathId>();
        TieringUsage = rowset.template GetValue<Schema::TableInfo::TieringUsage>();
        if (rowset.template HaveValue<Schema::TableInfo::DropStep>() && rowset.template HaveValue<Schema::TableInfo::DropTxId>()) {
            DropVersion.emplace();
            DropVersion->Step = rowset.template GetValue<Schema::TableInfo::DropStep>();
            DropVersion->TxId = rowset.template GetValue<Schema::TableInfo::DropTxId>();
        }
        return true;
    }
};

class TTablesManager {
private:
    THashMap<ui64, TTableInfo> Tables;
    THashMap<ui32, TSchemaPreset> SchemaPresets;
    THashSet<ui64> PathsToDrop;
    TTtl Ttl;
    std::unique_ptr<NOlap::IColumnEngine> PrimaryIndex;
    ui64 TabletId;
public:
    const TTtl& GetTtl() const {
        return Ttl;
    }

    void AddTtls(THashMap<ui64, NOlap::TTiering>& eviction, TInstant now, bool force) {
        Ttl.AddTtls(eviction, now, force);
    }

    const THashSet<ui64>& GetPathsToDrop() const {
        return PathsToDrop;
    }

    const THashMap<ui64, TTableInfo>& GetTables() const {
        return Tables;
    }

    const THashMap<ui32, TSchemaPreset>& GetSchemaPresets() const {
        return SchemaPresets;
    }

    bool IsOverloaded(const ui64 pathId) const {
        return PrimaryIndex && PrimaryIndex->HasOverloadedGranules(pathId);
    }

    bool HasPrimaryIndex() const {
        return !!PrimaryIndex;
    }

    NOlap::IColumnEngine& MutablePrimaryIndex() {
        Y_VERIFY(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    const NOlap::TIndexInfo& GetIndexInfo(const NOlap::TSnapshot& version = NOlap::TSnapshot::Zero()) const {
        Y_UNUSED(version);
        Y_VERIFY(!!PrimaryIndex);
        return PrimaryIndex->GetIndexInfo();
    }

    const std::unique_ptr<NOlap::IColumnEngine>& GetPrimaryIndex() const {
        return PrimaryIndex;
    }

    const NOlap::IColumnEngine& GetPrimaryIndexSafe() const {
        Y_VERIFY(!!PrimaryIndex);
        return *PrimaryIndex;
    }

    bool InitFromDB(NIceDb::TNiceDb& db, const ui64 tabletId);
    bool LoadIndex(NOlap::TDbWrapper& db, THashSet<NOlap::TUnifiedBlobId>& lostEvictions);

    void Clear();

    const TTableInfo& GetTable(const ui64 pathId) const;
    ui64 GetMemoryUsage() const;

    bool HasTable(const ui64 pathId) const;
    bool IsWritableTable(const ui64 pathId) const;
    bool HasPreset(const ui32 presetId) const;

    void DropTable(const ui64 pathId, const TRowVersion& version, NIceDb::TNiceDb& db);
    void DropPreset(const ui32 presetId, const TRowVersion& version, NIceDb::TNiceDb& db);

    void RegisterTable(TTableInfo&& table, NIceDb::TNiceDb& db);
    bool RegisterSchemaPreset(const TSchemaPreset& schemaPreset, NIceDb::TNiceDb& db);

    void AddPresetVersion(const ui32 presetId, const TRowVersion& version, const NKikimrSchemeOp::TColumnTableSchema& schema, NIceDb::TNiceDb& db);
    void AddTableVersion(const ui64 pathId, const TRowVersion& version, const TTableInfo::TTableVersionInfo& versionInfo, NIceDb::TNiceDb& db);

    void OnTtlUpdate();

    std::shared_ptr<NOlap::TColumnEngineChanges> StartIndexCleanup(const NOlap::TSnapshot& snapshot, const NOlap::TCompactionLimits& limits, ui32 maxRecords);

private:
    void IndexSchemaVersion(const TRowVersion& version, const NKikimrSchemeOp::TColumnTableSchema& schema);
    static NOlap::TIndexInfo DeserializeIndexInfoFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema);
};

}
