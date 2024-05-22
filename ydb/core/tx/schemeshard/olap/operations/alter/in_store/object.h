#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreTable: public ISSEntity {
private:
    using TBase = ISSEntity;
    YDB_READONLY_DEF(TColumnTableInfo::TPtr, TableInfo);
    YDB_READONLY_DEF(TOlapStoreInfo::TPtr, StoreInfo);
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const override;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
    [[nodiscard]] TConclusionStatus InitializeWithTableInfo(const TColumnTableInfo::TPtr& tInfo, const TEntityInitializationContext& context);
public:
    virtual TString GetClassName() const override {
        return "IN_STORE_TABLE";
    }

    TInStoreTable(const TPathId& pathId)
        : TBase(pathId) {

    }

    TInStoreTable(const TPathId& pathId, const TColumnTableInfo::TPtr& tInfo, const TEntityInitializationContext& context)
        : TBase(pathId) {
        InitializeWithTableInfo(tInfo, context).Validate();
    }

    const TColumnTableInfo& GetTableInfoVerified() const {
        AFL_VERIFY(!!TableInfo);
        return *TableInfo;
    }

    TColumnTableInfo::TPtr GetTableInfoPtrVerified() const {
        AFL_VERIFY(!!TableInfo);
        return TableInfo;
    }

    TConclusion<NKikimrSchemeOp::TColumnTableSchema> GetTableSchemaProto() const {
        AFL_VERIFY(!!StoreInfo);
        if (!StoreInfo->SchemaPresets.count(TableInfo->Description.GetSchemaPresetId())) {
            return TConclusionStatus::Fail("No preset for in-store column table");
        }

        auto& preset = StoreInfo->SchemaPresets.at(TableInfo->Description.GetSchemaPresetId());
        auto& presetProto = StoreInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
        if (!presetProto.HasSchema()) {
            return TConclusionStatus::Fail("No schema in preset for in-store column table");
        }
        return presetProto.GetSchema();
    }

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetTableTTLProto() {
        AFL_VERIFY(TableInfo);
        return TableInfo->Description.GetTtlSettings();
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