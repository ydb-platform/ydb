#pragma once
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <tuple>

namespace NKikimr::NOlap {
class TSchemaPresetVersionInfo {
private:
    NKikimrTxColumnShard::TSchemaPresetVersionInfo Proto;

public:
    TSchemaPresetVersionInfo(const NKikimrTxColumnShard::TSchemaPresetVersionInfo& proto)
        : Proto(proto) {
    }

    const NKikimrTxColumnShard::TSchemaPresetVersionInfo& GetProto() const {
        return Proto;
    }

    auto operator<=>(const TSchemaPresetVersionInfo& rhs) const {
        return std::tuple(Proto.GetId(), Proto.GetSinceStep(), Proto.GetSinceTxId()) <=> std::tuple(rhs.Proto.GetId(), rhs.Proto.GetSinceStep(), rhs.Proto.GetSinceTxId());
    }

    void SaveToLocalDb(NIceDb::TNiceDb& db) {
        using namespace NKikimr::NColumnShard;
        db.Table<Schema::SchemaPresetVersionInfo>().Key(Proto.GetId(), Proto.GetSinceStep(), Proto.GetSinceTxId()).Update<Schema::SchemaPresetVersionInfo::InfoProto>(Proto.SerializeAsString());
    }

    TSnapshot GetSnapshot() const {
        return TSnapshot(Proto.GetSinceStep(), Proto.GetSinceTxId());
    }

    NOlap::IColumnEngine::TSchemaInitializationData GetSchema() const {
        return NOlap::IColumnEngine::TSchemaInitializationData(Proto);
    }

    ui64 ColumnsSize() const {
        if (Proto.HasSchema()) {
            return Proto.GetSchema().ColumnsSize();
        }
        AFL_VERIFY(Proto.HasDiff());
        return Proto.GetDiff().UpsertColumnsSize() + Proto.GetDiff().UpsertIndexesSize();
    }
};
} // namespace NKikimr::NOlap
