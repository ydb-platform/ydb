#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/olap/ttl/schema.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TSSEntityColumnTable: public ISSEntity {
private:
    using TBase = ISSEntity;
    YDB_READONLY_DEF(TColumnTableInfo::TPtr, TableInfo);
protected:
    virtual std::shared_ptr<ISSEntityEvolution> DoGetCurrentEvolution() const override {
        return TableInfo->GetCurrentEvolution();
    }
    virtual std::shared_ptr<ISSEntityEvolution> DoExtractCurrentEvolution() override {
        return TableInfo->ExtractCurrentEvolution();
    }

    virtual void DoFinishUpdate() override {
        TableInfo->CleanEvolutions();
    }
    virtual void DoPersist(const TDBWriteContext& dbContext) const override;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
    [[nodiscard]] virtual TConclusionStatus DoStartUpdate(TEvolutions&& evolutions, const TStartUpdateContext& context) override;
public:
    TSSEntityColumnTable(const TPathId& pathId)
        : TBase(pathId) {

    }

    TSSEntityColumnTable(const TPathId& pathId, const TColumnTableInfo::TPtr& tInfo)
        : TBase(pathId)
        , TableInfo(tInfo)
    {
        AFL_VERIFY(!!TableInfo);
    }

    const TColumnTableInfo& GetTableInfoVerified() const {
        AFL_VERIFY(!!TableInfo);
        return *TableInfo;
    }

    TColumnTableInfo::TPtr GetTableInfoPtrVerified() const {
        AFL_VERIFY(!!TableInfo);
        return TableInfo;
    }

    const NKikimrSchemeOp::TColumnDataLifeCycle& GetTableTTLProto() {
        return GetTableInfoVerified().Description.GetTtlSettings();
    }

};

}