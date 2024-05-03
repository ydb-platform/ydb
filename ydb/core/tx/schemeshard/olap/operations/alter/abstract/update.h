#pragma once
#include "object.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityEvolution;

class ISSEntityUpdate {
private:
    YDB_READONLY_DEF(std::shared_ptr<ISSEntity>, OriginalEntity);
    bool Initialized = false;
protected:
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) = 0;
    virtual TConclusion<std::shared_ptr<ISSEntityEvolution>> DoBuildEvolution(const std::shared_ptr<ISSEntityUpdate>& selfPtr) const = 0;
public:
    virtual ~ISSEntityUpdate() = default;
    ISSEntityUpdate(const std::shared_ptr<ISSEntity>& originalEntity)
        : OriginalEntity(originalEntity) {
        AFL_VERIFY(OriginalEntity);
    }

    TConclusionStatus Initialize(const TUpdateInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        return DoInitialize(context);
    }

    TConclusion<std::shared_ptr<ISSEntityEvolution>> BuildEvolution(const std::shared_ptr<ISSEntityUpdate>& selfPtr) const {
        AFL_VERIFY(!!selfPtr);
        return DoBuildEvolution(selfPtr);
    }
};

}