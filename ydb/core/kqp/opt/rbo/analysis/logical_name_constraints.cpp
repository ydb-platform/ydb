#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

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

TVector<TInfoUnit> BuildMapOutput(const TVector<TInfoUnit>& inputOutput, const TVector<TMapElement>& elements) {
    TVector<TInfoUnit> output = inputOutput;
    TInfoUnitSet renameSources;
    for (const auto& element : elements) {
        if (element.IsRename()) {
            AddInfoUnit(renameSources, element.GetRename());
        }
    }

    if (!renameSources.empty()) {
        TVector<TInfoUnit> kept;
        kept.reserve(output.size());
        for (const auto& iu : output) {
            if (!renameSources.contains(iu)) {
                kept.push_back(iu);
            }
        }
        output = std::move(kept);
    }

    for (const auto& element : elements) {
        output.push_back(element.GetElementName());
    }

    return output;
}

TVector<TInfoUnit> BuildJoinOutput(
    const TString& joinKind,
    TVector<TInfoUnit> leftOutput,
    TVector<TInfoUnit> rightOutput)
{
    if (joinKind == "LeftOnly" || joinKind == "LeftSemi") {
        rightOutput.clear();
    }
    if (joinKind == "RightOnly" || joinKind == "RightSemi") {
        leftOutput.clear();
    }

    leftOutput.insert(leftOutput.end(), rightOutput.begin(), rightOutput.end());
    return leftOutput;
}

TVector<TInfoUnit> BuildAggregateOutput(const TOpAggregate& aggregate) {
    TVector<TInfoUnit> output;
    if (!aggregate.IsDistinctAll()) {
        output = aggregate.KeyColumns;
    }
    for (const auto& traits : aggregate.AggregationTraitsList) {
        output.push_back(traits.ResultColName);
    }
    return output;
}

const TVector<TInfoUnit>& GetChildOutput(
    const TIntrusivePtr<IOperator>& child,
    const THashMap<IOperator*, TVector<TInfoUnit>>& outputOverrides)
{
    const auto it = outputOverrides.find(child.get());
    return it == outputOverrides.end() ? child->GetOutputIUs() : it->second;
}

TVector<TInfoUnit> ComputeOutputWithOverrides(
    IOperator* op,
    const THashMap<IOperator*, TVector<TInfoUnit>>& outputOverrides)
{
    switch (op->Kind) {
        case EOperator::EmptySource:
            return {};
        case EOperator::Source:
            return static_cast<TOpRead*>(op)->OutputIUs;
        case EOperator::Map: {
            auto* map = static_cast<TOpMap*>(op);
            return BuildMapOutput(GetChildOutput(map->GetInput(), outputOverrides), map->MapElements);
        }
        case EOperator::AddDependencies: {
            auto* addDependencies = static_cast<TOpAddDependencies*>(op);
            TVector<TInfoUnit> output = GetChildOutput(addDependencies->GetInput(), outputOverrides);
            output.insert(output.end(), addDependencies->Dependencies.begin(), addDependencies->Dependencies.end());
            return output;
        }
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::Root:
            return GetChildOutput(static_cast<IUnaryOperator*>(op)->GetInput(), outputOverrides);
        case EOperator::Join: {
            auto* join = static_cast<TOpJoin*>(op);
            return BuildJoinOutput(
                join->JoinKind,
                GetChildOutput(join->GetLeftInput(), outputOverrides),
                GetChildOutput(join->GetRightInput(), outputOverrides));
        }
        case EOperator::UnionAll:
            return static_cast<TOpUnionAll*>(op)->Columns;
        case EOperator::Aggregate:
            return BuildAggregateOutput(*static_cast<TOpAggregate*>(op));
        case EOperator::CBOTree:
            return op->GetOutputIUs();
    }
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

bool CanReplaceOutputInParents(IOperator* oldOp, const TVector<TInfoUnit>& replacementOutput, const TPlanProps& props) {
    THashMap<IOperator*, TVector<TInfoUnit>> outputOverrides;
    TVector<IOperator*> queue;

    auto addOverride = [&outputOverrides, &queue](IOperator* op, const TVector<TInfoUnit>& output) {
        auto [it, inserted] = outputOverrides.emplace(op, output);
        if (inserted || it->second != output) {
            if (!inserted) {
                it->second = output;
            }
            queue.push_back(op);
        }
    };

    addOverride(oldOp, replacementOutput);

    for (size_t pos = 0; pos < queue.size(); ++pos) {
        auto* op = queue[pos];
        const auto& output = outputOverrides.at(op);
        if (!CanExposeOutput(op, output, props)) {
            return false;
        }

        for (const auto& [parent, _] : op->Parents) {
            addOverride(parent, ComputeOutputWithOverrides(parent, outputOverrides));
        }
    }

    return true;
}

bool CanReplaceOutputInParents(const TIntrusivePtr<IOperator>& oldOp, const TVector<TInfoUnit>& replacementOutput, const TPlanProps& props) {
    return CanReplaceOutputInParents(oldOp.get(), replacementOutput, props);
}

bool CanReplaceInParents(
    const TIntrusivePtr<IOperator>& oldOp,
    const TIntrusivePtr<IOperator>& replacement,
    const TPlanProps& props)
{
    return CanReplaceOutputInParents(oldOp, replacement->GetOutputIUs(), props);
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
    Y_UNUSED(ctx);
    return false;
}

void ComputePlanNameConstraints(TOpRoot& root) {
    TLogicalNameConstraints(root.PlanProps).Run(root);
}

} // namespace NKqp
} // namespace NKikimr
