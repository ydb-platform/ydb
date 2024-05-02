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
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const = 0;
public:
    virtual ~ISSEntity() = default;
    virtual TString GetClassName() const = 0;

    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> CreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
        return DoCreateUpdate(context, selfPtr);
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
};

}