#pragma once
#include <ydb/core/tx/schemeshard/operations/abstract/object.h>
#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TColumnTableEntity: public NOperations::ISSEntity {
private:
    using TBase = NOperations::ISSEntity;
    YDB_READONLY_DEF(TColumnTableInfo::TPtr, TableInfo);
    virtual TConclusionStatus DoInitializeImpl(const NOperations::TEntityInitializationContext& context) = 0;
    virtual TConclusion<std::shared_ptr<NOperations::ISSEntityUpdate>> DoCreateUpdateImpl(const NOperations::TUpdateInitializationContext& context) const = 0;
protected:
    TConclusion<std::shared_ptr<NOperations::ISSEntityUpdate>> DoRestoreUpdate(const NOperations::TUpdateRestoreContext& ssContext) const override;
    virtual TConclusionStatus DoInitialize(const NOperations::TEntityInitializationContext& context) override final;
    virtual TConclusion<std::shared_ptr<NOperations::ISSEntityUpdate>> DoCreateUpdate(const NOperations::TUpdateInitializationContext& context) const override final;;

    TColumnTableEntity(const TPathId& pathId, const TColumnTableInfo::TPtr& tabletInfo)
        : TBase(pathId)
        , TableInfo(tabletInfo) {
    }

public:

    TColumnTableEntity(const TPathId& pathId)
        : TBase(pathId) {
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
        AFL_VERIFY(TableInfo);
        return TableInfo->Description.GetTtlSettings();
    }

    static std::shared_ptr<NKikimr::NSchemeShard::NOperations::ISSEntity> GetEntityVerified(TOperationContext& context, const TPath& path);
};

}