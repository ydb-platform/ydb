#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneTable: public ISSEntity {
private:
    using TBase = ISSEntity;
    TColumnTableInfo::TPtr TableInfo;
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const override;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
public:
    TStandaloneTable(const TPathId& pathId)
        : TBase(pathId)
    {

    }
    TStandaloneTable(const TPathId& pathId, const TColumnTableInfo::TPtr& tableInfo)
        : TBase(pathId)
    {
        InitializeFromTableInfo(tableInfo).Validate();
    }

    TColumnTableInfo::TPtr GetTableInfoPtrVerified() const {
        AFL_VERIFY(!!TableInfo);
        return TableInfo;
    }

    virtual TString GetClassName() const override {
        return "STANDALONE_TABLE";
    }

    const NKikimrSchemeOp::TColumnTableSchema& GetTableSchemaProto() {
        AFL_VERIFY(TableInfo);
        return TableInfo->Description.GetSchema();
    }

    const TColumnTableInfo& GetTableInfoVerified() {
        AFL_VERIFY(TableInfo);
        return *TableInfo;
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

    TConclusionStatus InitializeFromTableInfo(const TColumnTableInfo::TPtr& tableInfo);

    
};

}