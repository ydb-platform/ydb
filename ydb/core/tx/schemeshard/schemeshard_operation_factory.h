#pragma once

#include <util/generic/ptr.h>
#include <util/system/types.h>
#include <util/system/compiler.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

class ISubOperation;
struct TOperation;
struct TOperationContext;
class TSealedOperationPlan;
struct TPartBlueprint;
class TOperationId;

class IOperationFactory {
protected:
    using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

public:
    virtual ~IOperationFactory() = default;

    // Returns operation parts for given tx (commonly identified by tx/operation type).
    // Used to customize parts/behaviour.
    virtual TVector<TIntrusivePtr<ISubOperation>> MakeOperationParts(
        const TOperation& op,
        const TTxTransaction& tx,
        TOperationContext& ctx) const = 0;

    // Builds the one part a blueprint of a sealed plan describes. The part is constructed
    // from the blueprint's transaction and later bound to the plan by the caller.
    virtual TIntrusivePtr<ISubOperation> MakePlannedPart(
        const TOperationId& id,
        const TSealedOperationPlan& plan,
        const TPartBlueprint& blueprint,
        TOperationContext& ctx) const = 0;

    // Observation point: called with the sealed plan before any part of the operation is
    // constructed, and therefore before its first propose.
    virtual void OnPlanSealed(ui64 txId, const TSealedOperationPlan& plan) const {
        Y_UNUSED(txId);
        Y_UNUSED(plan);
    }
};

IOperationFactory* DefaultOperationFactory();

}
