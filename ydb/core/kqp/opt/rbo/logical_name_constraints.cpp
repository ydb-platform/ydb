#include "kqp_rbo.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool AddInfoUnit(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

bool AddColumnsToSet(TInfoUnitSet& target, const TVector<TInfoUnit>& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddInfoUnit(target, iu);
    }
    return changed;
}

bool AddColumnsToSet(TInfoUnitSet& target, const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddInfoUnit(target, iu);
    }
    return changed;
}

TInfoUnitSet MakeInfoUnitSet(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    AddColumnsToSet(result, ius);
    return result;
}

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
            AddColumnsToSet(result, Props.NameConstraints.GetForbiddenOut(parent, childIdx, op));
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

} // anonymous namespace

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
        AddColumnsToSet(leftForbidden, rightOutput);
        AddColumnsToSet(rightForbidden, leftOutput);
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
