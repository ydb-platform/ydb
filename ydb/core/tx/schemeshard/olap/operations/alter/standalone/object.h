#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/common/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneTable: public TColumnTableEntity {
private:
    using TBase = TColumnTableEntity;
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdateImpl(const TUpdateInitializationContext& context) const override;
    virtual TConclusionStatus DoInitializeImpl(const TEntityInitializationContext& context) override;
    TConclusionStatus InitializeFromTableInfo();
public:
    TStandaloneTable(const TPathId& pathId)
        : TBase(pathId)
    {

    }

    TStandaloneTable(const TPathId& pathId, const std::shared_ptr<TColumnTableInfo>& tableInfo)
        : TBase(pathId, tableInfo)
    {
        InitializeFromTableInfo();
    }

    virtual TString GetClassName() const override {
        return "STANDALONE_TABLE";
    }

    const NKikimrSchemeOp::TColumnTableSchema& GetTableSchemaProto() const {
        AFL_VERIFY(GetTableInfo());
        return GetTableInfo()->Description.GetSchema();
    }

    const TColumnTableInfo& GetTableInfoVerified() const {
        AFL_VERIFY(GetTableInfo());
        return *GetTableInfo();
    }

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetTableTTLProto() const {
        AFL_VERIFY(GetTableInfo());
        return GetTableInfo()->Description.GetTtlSettings();
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