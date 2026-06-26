#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

namespace NKikimr {
namespace NKqp {

namespace {

const TPlanNameConstraints& GetComputedNameConstraints(IOperator* op) {
    Y_ENSURE(op);
    Y_ENSURE(op->Props.Analysis.NameConstraints.has_value(), "Name constraints requested for an operator without computed constraints");
    return *op->Props.Analysis.NameConstraints;
}

TPlanNameConstraints& GetMutableComputedNameConstraints(IOperator* op) {
    Y_ENSURE(op);
    Y_ENSURE(op->Props.Analysis.NameConstraints.has_value(), "Name constraints update requested for an operator without computed constraints");
    return *op->Props.Analysis.NameConstraints;
}

class TLogicalNameConstraints: public INameConstraintsContext {
public:
    void Run(TOpRoot& root) {
        for (const auto& iter : root) {
            iter.Current->Props.Analysis.NameConstraints.emplace();
        }

        bool changed = true;
        ui32 iteration = 0;
        while (changed) {
            changed = false;
            for (const auto& iter : root) {
                changed |= iter.Current->PropagateNameConstraints(*this);
            }
            Y_ENSURE(++iteration < 1000, "Name constraint propagation did not converge");
        }
    }

    TInfoUnitSet GetIncomingForbidden(IOperator* op) const override {
        TInfoUnitSet result;
        for (const auto& [parent, childIdx] : op->Parents) {
            AddInfoUnits(result, GetComputedNameConstraints(op).GetForbiddenOut(parent, childIdx, op));
        }
        return result;
    }

    bool AddForbiddenToChild(IOperator* parent, ui32 childIdx, const TInfoUnitSet& forbidden) override {
        if (forbidden.empty()) {
            return false;
        }
        Y_ENSURE(childIdx < parent->Children.size());
        auto child = parent->Children[childIdx];
        return GetMutableComputedNameConstraints(child.get()).AddForbiddenOut(parent, childIdx, child.get(), forbidden);
    }
};

ui32 ResolveChildIdx(IOperator* from, IOperator* to) {
    Y_ENSURE(from);
    Y_ENSURE(to);

    ui32 result = 0;
    ui32 matches = 0;
    for (ui32 childIdx = 0; childIdx < to->Children.size(); ++childIdx) {
        if (to->Children[childIdx].get() == from) {
            result = childIdx;
            ++matches;
        }
    }

    Y_ENSURE(matches == 1, "Expected exactly one edge from producer to consumer");
    return result;
}

} // anonymous namespace

void TPlanNameConstraints::Clear() {
    ForbiddenOut.clear();
}

bool TPlanNameConstraints::AddForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child, const TInfoUnit& iu) {
    Y_ENSURE(child);
    return ForbiddenOut[TPlanEdgeKey{parent, childIdx, child}].insert(iu).second;
}

bool TPlanNameConstraints::AddForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child, const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddForbiddenOut(parent, childIdx, child, iu);
    }
    return changed;
}

const TInfoUnitSet& TPlanNameConstraints::GetForbiddenOut(IOperator* parent, ui32 childIdx, IOperator* child) const {
    const auto it = ForbiddenOut.find(TPlanEdgeKey{parent, childIdx, child});
    return it == ForbiddenOut.end() ? EmptyInfoUnitSet() : it->second;
}

bool IOperator::PropagateNameConstraints(INameConstraintsContext& ctx) {
    Y_UNUSED(ctx);
    return false;
}

bool IUnaryOperator::PropagateNameConstraints(INameConstraintsContext& ctx) {
    auto childForbidden = ctx.GetIncomingForbidden(this);
    if (Kind == EOperator::AddDependencies) {
        AddInfoUnits(childForbidden, static_cast<TOpAddDependencies*>(this)->Dependencies);
    }

    return ctx.AddForbiddenToChild(this, 0, childForbidden);
}

bool TOpMap::PropagateNameConstraints(INameConstraintsContext& ctx) {
    auto childForbidden = ctx.GetIncomingForbidden(this);
    TInfoUnitSet renameSources;
    TInfoUnitSet mapElementOutputs;
    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            AddInfoUnit(renameSources, mapElement.GetRename());
        }
        AddInfoUnit(mapElementOutputs, mapElement.GetElementName());
    }

    for (const auto& iu : renameSources) {
        childForbidden.erase(iu);
    }
    for (const auto& iu : mapElementOutputs) {
        if (!renameSources.contains(iu)) {
            AddInfoUnit(childForbidden, iu);
        }
    }

    return ctx.AddForbiddenToChild(this, 0, childForbidden);
}

bool TOpAggregate::PropagateNameConstraints(INameConstraintsContext& ctx) {
    Y_UNUSED(ctx);
    return false;
}

bool TOpJoin::PropagateNameConstraints(INameConstraintsContext& ctx) {
    const bool outputsLeft = JoinKind != "RightOnly" && JoinKind != "RightSemi";
    const bool outputsRight = JoinKind != "LeftOnly" && JoinKind != "LeftSemi";

    const auto incoming = ctx.GetIncomingForbidden(this);
    const auto leftOutput = GetLeftInput()->GetOutputIUs();
    const auto rightOutput = GetRightInput()->GetOutputIUs();

    TInfoUnitSet leftForbidden;
    TInfoUnitSet rightForbidden;

    if (outputsLeft) {
        AddInfoUnits(leftForbidden, incoming);
    }
    if (outputsRight) {
        AddInfoUnits(rightForbidden, incoming);
    }

    if (outputsLeft && outputsRight) {
        AddInfoUnits(leftForbidden, rightOutput);
        AddInfoUnits(rightForbidden, leftOutput);
    }

    bool changed = false;
    changed |= ctx.AddForbiddenToChild(this, 0, leftForbidden);
    changed |= ctx.AddForbiddenToChild(this, 1, rightForbidden);
    return changed;
}

bool TOpUnionAll::PropagateNameConstraints(INameConstraintsContext& ctx) {
    Y_UNUSED(ctx);
    return false;
}

void ComputePlanNameConstraints(TOpRoot& root) {
    TLogicalNameConstraints().Run(root);
}

const TInfoUnitSet& GetForbidden(
    IOperator* from,
    IOperator* to)
{
    const ui32 childIdx = ResolveChildIdx(from, to);
    return GetComputedNameConstraints(from).GetForbiddenOut(to, childIdx, from);
}

TInfoUnitSet GetForbidden(
    IOperator* op)
{
    Y_ENSURE(op);

    TInfoUnitSet result;
    for (const auto& [parent, childIdx] : op->Parents) {
        AddInfoUnits(result, GetComputedNameConstraints(op).GetForbiddenOut(parent, childIdx, op));
    }
    return result;
}

} // namespace NKqp
} // namespace NKikimr
