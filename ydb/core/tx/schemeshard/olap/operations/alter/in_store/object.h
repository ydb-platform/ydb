#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/common/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreTable: public TColumnTableEntity {
private:
    using TBase = TColumnTableEntity;
    YDB_READONLY_DEF(TOlapStoreInfo::TPtr, StoreInfo);
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdateImpl(const TUpdateInitializationContext& context) const override;
    virtual TConclusionStatus DoInitializeImpl(const TEntityInitializationContext& context) override;
    [[nodiscard]] TConclusionStatus InitializeWithTableInfo(const TEntityInitializationContext& context);
public:
    virtual TString GetClassName() const override {
        return "IN_STORE_TABLE";
    }

    using TBase::TBase;

    TInStoreTable(const TPathId& pathId, const std::shared_ptr<TColumnTableInfo>& tableInfo, const TEntityInitializationContext& context)
        : TBase(pathId, tableInfo)
    {
        InitializeWithTableInfo(context).Validate();
    }

    TConclusion<NKikimrSchemeOp::TColumnTableSchema> GetTableSchemaProto() const;

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetTableTTLProto() {
        return GetTableInfoPtrVerified()->Description.GetTtlSettings();
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