#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TColumnTableEntity: public ISSEntity {
private:
    using TBase = ISSEntity;
    YDB_READONLY_DEF(TColumnTableInfo::TPtr, TableInfo);
    virtual TConclusionStatus DoInitializeImpl(const TEntityInitializationContext& context) = 0;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoBuildUpdate(const NKikimrSchemeOp::TModifyScheme& modification) const = 0;

    template <class TInitializer>
    TConclusion<std::shared_ptr<ISSEntityUpdate>> BuildUpdate(
        const NKikimrSchemeOp::TModifyScheme& modification, TInitializer&& initializer) const;

protected:
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoRestoreUpdate(const TUpdateRestoreContext& ssContext) const override final;
    virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override final;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context) const override final;

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
};

}