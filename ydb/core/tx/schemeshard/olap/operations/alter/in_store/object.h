#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/table/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreTable: public TSSEntityColumnTable {
private:
    using TBase = TSSEntityColumnTable;
    YDB_READONLY_DEF(TOlapStoreInfo::TPtr, StoreInfo);
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const override;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
    [[nodiscard]] TConclusionStatus InitializeWithTableInfo(const TEntityInitializationContext& context);
    [[nodiscard]] virtual TConclusionStatus DoStartUpdate(TEvolutions&& evolutions, const TStartUpdateContext& context) override;

public:
    virtual TString GetClassName() const override {
        return "IN_STORE_TABLE";
    }

    virtual std::set<ui64> GetShardIds() const override {
        return GetTableInfoVerified().GetShardIdsSet();
    }

    TInStoreTable(const TPathId& pathId)
        : TBase(pathId) {

    }

    TInStoreTable(const TPathId& pathId, const TColumnTableInfo::TPtr& tInfo, const TEntityInitializationContext& context)
        : TBase(pathId, tInfo) {
        InitializeWithTableInfo(context).Validate();
    }

    TConclusion<NKikimrSchemeOp::TColumnTableSchema> GetTableSchemaProto() const {
        AFL_VERIFY(!!StoreInfo);
        if (!StoreInfo->SchemaPresets.count(GetTableInfoVerified().Description.GetSchemaPresetId())) {
            return TConclusionStatus::Fail("No preset for in-store column table");
        }

        auto& preset = StoreInfo->SchemaPresets.at(GetTableInfoVerified().Description.GetSchemaPresetId());
        auto& presetProto = StoreInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
        if (!presetProto.HasSchema()) {
            return TConclusionStatus::Fail("No schema in preset for in-store column table");
        }
        return presetProto.GetSchema();
    }

    const TOlapSchema& GetTableSchemaVerified() const {
        AFL_VERIFY(!!TableSchema);
        return *TableSchema;
    }

    const TOlapTTL* GetTableTTLOptional() const {
        return TableTTL ? &*TableTTL : nullptr;
    }
};

}