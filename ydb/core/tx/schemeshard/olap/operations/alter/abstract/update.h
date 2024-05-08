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
    YDB_READONLY_DEF(NKikimrSchemeOp::TModifyScheme, Request);
    bool Initialized = false;
protected:
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) = 0;
    virtual TConclusion<TEvolutions> DoBuildEvolutions() const = 0;
    virtual TVector<ISubOperation::TPtr> DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const = 0;
public:
    virtual ~ISSEntityUpdate() = default;

    ISSEntityUpdate(const std::shared_ptr<ISSEntity>& originalEntity, const NKikimrSchemeOp::TModifyScheme& request)
        : OriginalEntity(originalEntity)
        , Request(request)
    {
        AFL_VERIFY(OriginalEntity);
    }

    TConclusionStatus Initialize(const TUpdateInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        return DoInitialize(context);
    }

    TConclusion<TEvolutions> BuildEvolutions() const {
        AFL_VERIFY(Initialized);
        return DoBuildEvolutions();
    }

    TVector<ISubOperation::TPtr> BuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const {
        AFL_VERIFY(Initialized);
        return DoBuildOperations(id, request);
    }

};

}