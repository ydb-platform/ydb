#pragma once
#include "context.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityUpdate;

class ISSEntity {
private:
    YDB_READONLY_DEF(TPathId, PathId);
    bool Initialized = false;
protected:
    [[nodiscard]] virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) = 0;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context) const = 0;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoRestoreUpdate(const TUpdateRestoreContext& context) const = 0;

public:
    virtual ~ISSEntity() = default;
    virtual TString GetClassName() const = 0;

    TConclusion<std::shared_ptr<ISSEntityUpdate>> CreateUpdate(const TUpdateInitializationContext& context) const {
        return DoCreateUpdate(context);
    }

    TConclusion<std::shared_ptr<ISSEntityUpdate>> RestoreUpdate(const TUpdateRestoreContext& context) const {
        return DoRestoreUpdate(context);
    }

    std::shared_ptr<ISSEntityUpdate> RestoreUpdateVerified(const TUpdateRestoreContext& context) const {
        return RestoreUpdate(context).DetachResult();
    }

    [[nodiscard]] TConclusionStatus Initialize(const TEntityInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        return DoInitialize(context);
    }

    ISSEntity(const TPathId& pathId)
        : PathId(pathId)
    {

    }

    static std::shared_ptr<ISSEntity> GetEntityVerified(TOperationContext& context, const TPath& path);
};

}