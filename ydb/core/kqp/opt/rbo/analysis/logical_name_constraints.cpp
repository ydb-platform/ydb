#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <tuple>

namespace NKikimr {
namespace NKqp {

namespace {

class TLogicalNameConstraints: public INameConstraintsContext {
public:
    explicit TLogicalNameConstraints(TPlanProps& props)
        : Props(props) {
    }

    void Run(TOpRoot& root) {
        Props.NameConstraints.Clear();

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
            AddInfoUnits(result, Props.NameConstraints.GetForbiddenOut(parent, childIdx, op));
        }
        return result;
    }

    bool AddForbiddenToChild(IOperator* parent, ui32 childIdx, const TInfoUnitSet& forbidden) override {
        if (forbidden.empty()) {
            return false;
        }
        Y_ENSURE(childIdx < parent->Children.size());
        return Props.NameConstraints.AddForbiddenOut(parent, childIdx, parent->Children[childIdx].get(), forbidden);
    }

private:
    TPlanProps& Props;
};

bool CanExposeToParents(IOperator* op, const TPlanProps& props, THashSet<IOperator*>& visited) {
    if (!op || !visited.insert(op).second) {
        return true;
    }

    if (!CanExposeOutput(op, op->GetOutputIUs(), props)) {
        return false;
    }

    for (const auto& [parent, _] : op->Parents) {
        if (!CanExposeToParents(parent, props, visited)) {
            return false;
        }
    }

    return true;
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

const TInfoUnitSet& TPlanNameConstraints::GetForbiddenOut(IOperator* parent, ui32 childIdx) const {
    Y_ENSURE(parent);
    Y_ENSURE(childIdx < parent->Children.size());
    return GetForbiddenOut(parent, childIdx, parent->Children[childIdx].get());
}

const TInfoUnitSet& TPlanNameConstraints::GetForbiddenOutForSingleConsumer(IOperator* op) const {
    if (!op || op->Parents.size() != 1) {
        return EmptyInfoUnitSet();
    }

    const auto& [parent, childIdx] = op->Parents.front();
    return GetForbiddenOut(parent, childIdx, op);
}

bool TPlanNameConstraints::IsForbiddenAtOutput(IOperator* op, const TInfoUnit& iu) const {
    if (!op) {
        return false;
    }

    for (const auto& [parent, childIdx] : op->Parents) {
        if (GetForbiddenOut(parent, childIdx, op).contains(iu)) {
            return true;
        }
    }

    return false;
}

bool HasOutputConflicts(const TVector<TInfoUnit>& outputIUs) {
    TInfoUnitSet seen;
    for (const auto& iu : outputIUs) {
        if (!seen.insert(iu).second) {
            return true;
        }
    }
    return false;
}

bool CanExposeOutput(IOperator* op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    if (HasOutputConflicts(outputIUs)) {
        return false;
    }

    for (const auto& [parent, childIdx] : op->Parents) {
        const auto& forbidden = props.NameConstraints.GetForbiddenOut(parent, childIdx, op);
        for (const auto& iu : outputIUs) {
            if (forbidden.contains(iu)) {
                return false;
            }
        }
    }

    return true;
}

bool CanExposeOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    return CanExposeOutput(op.get(), outputIUs, props);
}

bool CanExposeToParents(IOperator* op, const TPlanProps& props) {
    THashSet<IOperator*> visited;
    return CanExposeToParents(op, props, visited);
}

bool CanReplaceInParents(
    const TIntrusivePtr<IOperator>& oldOp,
    const TIntrusivePtr<IOperator>& replacement,
    const TPlanProps& props)
{
    if (!CanExposeOutput(oldOp, replacement->GetOutputIUs(), props)) {
        return false;
    }

    TVector<std::tuple<IOperator*, ui32, TIntrusivePtr<IOperator>>> oldChildren;
    oldChildren.reserve(oldOp->Parents.size());
    for (const auto& [parent, childIdx] : oldOp->Parents) {
        oldChildren.emplace_back(parent, childIdx, parent->Children[childIdx]);
        parent->Children[childIdx] = replacement;
    }

    bool valid = true;
    THashSet<IOperator*> visited;
    for (const auto& [parent, _] : oldOp->Parents) {
        if (!CanExposeToParents(parent, props, visited)) {
            valid = false;
            break;
        }
    }

    for (const auto& [parent, childIdx, oldChild] : oldChildren) {
        parent->Children[childIdx] = oldChild;
    }

    return valid;
}

bool IOperator::PropagateNameConstraints(INameConstraintsContext& ctx) {
    Y_UNUSED(ctx);
    return false;
}

bool IUnaryOperator::PropagateNameConstraints(INameConstraintsContext& ctx) {
    const auto incoming = ctx.GetIncomingForbidden(this);
    const auto inputOutput = MakeInfoUnitSet(GetInput()->GetOutputIUs());

    TInfoUnitSet childForbidden;
    for (const auto& iu : incoming) {
        if (inputOutput.contains(iu)) {
            AddInfoUnit(childForbidden, iu);
        }
    }

    return ctx.AddForbiddenToChild(this, 0, childForbidden);
}

bool TOpMap::PropagateNameConstraints(INameConstraintsContext& ctx) {
    const auto incoming = ctx.GetIncomingForbidden(this);
    const auto inputOutput = MakeInfoUnitSet(GetInput()->GetOutputIUs());

    TInfoUnitSet renameSources;
    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            AddInfoUnit(renameSources, mapElement.GetRename());
        }
    }

    TInfoUnitSet childForbidden;
    for (const auto& iu : incoming) {
        if (inputOutput.contains(iu) && !renameSources.contains(iu)) {
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
    const auto leftOutputSet = MakeInfoUnitSet(leftOutput);
    const auto rightOutputSet = MakeInfoUnitSet(rightOutput);

    TInfoUnitSet leftForbidden;
    TInfoUnitSet rightForbidden;

    if (outputsLeft && outputsRight) {
        AddInfoUnits(leftForbidden, rightOutput);
        AddInfoUnits(rightForbidden, leftOutput);
    }

    for (const auto& iu : incoming) {
        if (outputsLeft && leftOutputSet.contains(iu)) {
            AddInfoUnit(leftForbidden, iu);
        }
        if (outputsRight && rightOutputSet.contains(iu)) {
            AddInfoUnit(rightForbidden, iu);
        }
    }

    bool changed = false;
    changed |= ctx.AddForbiddenToChild(this, 0, leftForbidden);
    changed |= ctx.AddForbiddenToChild(this, 1, rightForbidden);
    return changed;
}

bool TOpUnionAll::PropagateNameConstraints(INameConstraintsContext& ctx) {
    const auto incoming = ctx.GetIncomingForbidden(this);
    const auto schema = MakeInfoUnitSet(GetOutputIUs());

    bool changed = false;
    for (ui32 childIdx = 0; childIdx < Children.size(); ++childIdx) {
        const auto& child = Children[childIdx];
        const auto childOutput = child->GetOutputIUs();
        const auto childOutputSet = MakeInfoUnitSet(childOutput);

        TInfoUnitSet childForbidden;
        for (const auto& iu : childOutput) {
            if (!schema.contains(iu)) {
                AddInfoUnit(childForbidden, iu);
            }
        }
        for (const auto& iu : incoming) {
            if (childOutputSet.contains(iu)) {
                AddInfoUnit(childForbidden, iu);
            }
        }

        changed |= ctx.AddForbiddenToChild(this, childIdx, childForbidden);
    }

    return changed;
}

void ComputePlanNameConstraints(TOpRoot& root) {
    TLogicalNameConstraints(root.PlanProps).Run(root);
}

} // namespace NKqp
} // namespace NKikimr
