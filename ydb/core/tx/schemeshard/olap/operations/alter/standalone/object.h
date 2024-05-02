#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/table/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneTable: public TSSEntityColumnTable {
private:
    using TBase = TSSEntityColumnTable;
    TColumnTableInfo::TPtr TableInfo;
    std::optional<TOlapSchema> TableSchema;
    std::optional<TOlapTTL> TableTTL;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const override;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
    virtual std::shared_ptr<ISSEntityEvolution> DoGetCurrentEvolution() const override {
        return TableInfo->GetCurrentEvolution();
    }
    virtual std::shared_ptr<ISSEntityEvolution> DoExtractCurrentEvolution() override {
        return TableInfo->ExtractCurrentEvolution();
    }
    TConclusionStatus InitializeFromTableInfo();

public:
    TStandaloneTable(const TPathId& pathId)
        : TBase(pathId)
    {

    }
    TStandaloneTable(const TPathId& pathId, const TColumnTableInfo::TPtr& tableInfo)
        : TBase(pathId, tableInfo)
    {
        InitializeFromTableInfo().Validate();
    }

    virtual std::set<ui64> GetShardIds() const override {
        return TableInfo->GetShardIdsSet();
    }

    virtual TString GetClassName() const override {
        return "STANDALONE_TABLE";
    }

    const TOlapTTL* GetTableTTLOptional() const {
        return TableTTL ? &*TableTTL : nullptr;
    }

};

}