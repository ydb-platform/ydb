#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <optional>

namespace NKikimr {
namespace NKqp {

namespace {

bool SameInfoUnitSet(const TInfoUnitSet& lhs, const TInfoUnitSet& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (const auto& iu : lhs) {
        if (!rhs.contains(iu)) {
            return false;
        }
    }
    return true;
}

TInfoUnitSet IntersectInfoUnitSets(const TInfoUnitSet& lhs, const TInfoUnitSet& rhs) {
    const auto* smaller = &lhs;
    const auto* larger = &rhs;
    if (smaller->size() > larger->size()) {
        std::swap(smaller, larger);
    }

    TInfoUnitSet result;
    for (const auto& iu : *smaller) {
        if (larger->contains(iu)) {
            AddInfoUnit(result, iu);
        }
    }
    return result;
}

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

bool PropagateSideBySideInputConflicts(IOperator& consumer);

bool PropagateForbidden(const TIntrusivePtr<IOperator>& op, const TInfoUnitConstraintSet& forbidden) {
    if (forbidden.Empty()) {
        return false;
    }
    return GetMutableComputedNameConstraints(op.get()).AddForbidden(forbidden);
}

class TLogicalNameConstraints {
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
                changed |= iter.Current->PropagateNameConstraints();
            }
            for (const auto& iter : root) {
                changed |= PropagateSideBySideInputConflicts(*iter.Current);
            }
            Y_ENSURE(++iteration < 1000, "Name constraint propagation did not converge");
        }
    }
};

} // anonymous namespace

bool TInfoUnitConstraintSet::UnionWith(const TInfoUnit& iu) {
    if (AllExcept_) {
        return Units_.erase(iu) != 0;
    }
    return Units_.insert(iu).second;
}

bool TInfoUnitConstraintSet::UnionWith(const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= UnionWith(iu);
    }
    return changed;
}

bool TInfoUnitConstraintSet::UnionWith(const TInfoUnitConstraintSet& other) {
    if (!other.AllExcept_) {
        return UnionWith(other.Units_);
    }

    TInfoUnitSet newUnits;
    if (AllExcept_) {
        newUnits = IntersectInfoUnitSets(Units_, other.Units_);
    } else {
        newUnits = other.Units_;
        for (const auto& iu : Units_) {
            newUnits.erase(iu);
        }
    }

    const bool changed = !AllExcept_ || !SameInfoUnitSet(Units_, newUnits);
    AllExcept_ = true;
    Units_ = std::move(newUnits);
    return changed;
}

bool TInfoUnitConstraintSet::Subtract(const TInfoUnit& iu) {
    if (AllExcept_) {
        return Units_.insert(iu).second;
    }
    return Units_.erase(iu) != 0;
}

bool TInfoUnitConstraintSet::Subtract(const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= Subtract(iu);
    }
    return changed;
}

bool TInfoUnitConstraintSet::IntersectWith(const TInfoUnitConstraintSet& other) {
    TInfoUnitConstraintSet result;
    if (!AllExcept_ && !other.AllExcept_) {
        result.Units_ = IntersectInfoUnitSets(Units_, other.Units_);
    } else if (!AllExcept_ && other.AllExcept_) {
        result.Units_ = Units_;
        for (const auto& iu : other.Units_) {
            result.Units_.erase(iu);
        }
    } else if (AllExcept_ && !other.AllExcept_) {
        result.Units_ = other.Units_;
        for (const auto& iu : Units_) {
            result.Units_.erase(iu);
        }
    } else {
        result.AllExcept_ = true;
        result.Units_ = Units_;
        AddInfoUnits(result.Units_, other.Units_);
    }

    const bool changed = AllExcept_ != result.AllExcept_ || !SameInfoUnitSet(Units_, result.Units_);
    *this = std::move(result);
    return changed;
}

TInfoUnitConstraintSet TInfoUnitConstraintSet::Complement() const {
    if (AllExcept_) {
        TInfoUnitConstraintSet result;
        result.Units_ = Units_;
        return result;
    }
    return TInfoUnitConstraintSet::AllExcept(Units_);
}

void TPlanNameConstraints::Clear() {
    Forbidden = {};
}

bool TPlanNameConstraints::AddForbidden(const TInfoUnitConstraintSet& forbidden) {
    return Forbidden.UnionWith(forbidden);
}

const TInfoUnitConstraintSet& TPlanNameConstraints::GetForbidden() const {
    return Forbidden;
}

namespace {

TInfoUnitConstraintSet MakeAllNamesConstraint() {
    return TInfoUnitConstraintSet::AllExcept({});
}

TInfoUnitConstraintSet GetMapHiddenInputNames(const TOpMap& map) {
    TInfoUnitConstraintSet result;
    for (const auto& mapElement : map.MapElements) {
        if (mapElement.IsRename()) {
            result.UnionWith(mapElement.GetRename());
        }
    }
    return result;
}

TInfoUnitConstraintSet GetHiddenNamesOnEdge(IOperator* parent, ui32 childIdx) {
    switch (parent->Kind) {
        case EOperator::Map:
            return GetMapHiddenInputNames(*static_cast<TOpMap*>(parent));
        case EOperator::Aggregate:
            return TInfoUnitConstraintSet::AllExcept(MakeInfoUnitSet(static_cast<TOpAggregate*>(parent)->GetKeyColumns()));
        case EOperator::UnionAll:
            return MakeAllNamesConstraint();
        case EOperator::Join: {
            const auto* join = static_cast<TOpJoin*>(parent);
            if ((childIdx == 0 && JoinOutputsLeft(join->JoinKind)) ||
                (childIdx == 1 && JoinOutputsRight(join->JoinKind)))
            {
                return {};
            }
            return MakeAllNamesConstraint();
        }
        default:
            return {};
    }
}

struct TSideBySideInputBranches {
    TIntrusivePtr<IOperator> First;
    TIntrusivePtr<IOperator> Second;
};

std::optional<TSideBySideInputBranches> GetSideBySideInputBranches(IOperator& consumer) {
    if (consumer.Kind == EOperator::Join) {
        auto& join = static_cast<TOpJoin&>(consumer);
        if (JoinOutputsLeft(join.JoinKind) && JoinOutputsRight(join.JoinKind)) {
            return TSideBySideInputBranches{
                join.GetLeftInput(),
                join.GetRightInput()
            };
        }
    }

    return std::nullopt;
}

struct TBranchHiddenState {
    std::optional<TInfoUnitConstraintSet> First;
    std::optional<TInfoUnitConstraintSet> Second;
};

struct TBranchQueueItem {
    IOperator* Op;
    ui32 BranchIdx;
    TInfoUnitConstraintSet Hidden;
};

std::optional<TInfoUnitConstraintSet>& GetBranchHidden(TBranchHiddenState& state, ui32 branchIdx) {
    return branchIdx == 0 ? state.First : state.Second;
}

bool PropagateSideBySideInputConflicts(IOperator& consumer) {
    const auto branches = GetSideBySideInputBranches(consumer);
    if (!branches) {
        return false;
    }

    THashMap<IOperator*, TBranchHiddenState> states;
    TVector<TBranchQueueItem> queue;

    queue.push_back({branches->First.get(), 0, {}});
    queue.push_back({branches->Second.get(), 1, {}});

    // Track names hidden on every path from a side-by-side input branch back to
    // each producer. If both branches reach the same producer, any name hidden
    // on neither branch would be exposed through both outputs and must stay forbidden.
    for (size_t index = 0; index < queue.size(); ++index) {
        auto item = std::move(queue[index]);
        auto& hiddenOnEveryPath = GetBranchHidden(states[item.Op], item.BranchIdx);
        bool changed = false;
        if (!hiddenOnEveryPath) {
            hiddenOnEveryPath = std::move(item.Hidden);
            changed = true;
        } else {
            // A name is hidden before this branch only if every path to the branch hides it.
            changed = hiddenOnEveryPath->IntersectWith(item.Hidden);
        }

        if (!changed) {
            continue;
        }

        for (ui32 childIdx = 0; childIdx < item.Op->Children.size(); ++childIdx) {
            auto childHidden = *hiddenOnEveryPath;
            childHidden.UnionWith(GetHiddenNamesOnEdge(item.Op, childIdx));
            queue.push_back({item.Op->Children[childIdx].get(), item.BranchIdx, std::move(childHidden)});
        }
    }

    bool changed = false;
    for (const auto& [producer, state] : states) {
        if (!state.First || !state.Second) {
            continue;
        }

        auto safeToExpose = *state.First;
        safeToExpose.UnionWith(*state.Second);
        changed |= GetMutableComputedNameConstraints(producer).AddForbidden(safeToExpose.Complement());
    }
    return changed;
}

} // anonymous namespace

bool IOperator::PropagateNameConstraints() {
    return false;
}

bool IUnaryOperator::PropagateNameConstraints() {
    auto childForbidden = GetForbidden(this);
    if (Kind == EOperator::AddDependencies) {
        childForbidden.UnionWith(MakeInfoUnitSet(static_cast<TOpAddDependencies*>(this)->Dependencies));
    }

    return PropagateForbidden(GetInput(), childForbidden);
}

bool TOpMap::PropagateNameConstraints() {
    auto childForbidden = GetForbidden(this);
    TInfoUnitSet renameSources;
    TInfoUnitSet mapElementOutputs;
    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            AddInfoUnit(renameSources, mapElement.GetRename());
        }
        AddInfoUnit(mapElementOutputs, mapElement.GetElementName());
    }

    for (const auto& iu : renameSources) {
        childForbidden.Subtract(iu);
    }
    for (const auto& iu : mapElementOutputs) {
        if (!renameSources.contains(iu)) {
            childForbidden.UnionWith(iu);
        }
    }

    return PropagateForbidden(GetInput(), childForbidden);
}

bool TOpAggregate::PropagateNameConstraints() {
    return false;
}

bool TOpJoin::PropagateNameConstraints() {
    const bool outputsLeft = JoinOutputsLeft(JoinKind);
    const bool outputsRight = JoinOutputsRight(JoinKind);

    const auto& incoming = GetForbidden(this);

    TInfoUnitConstraintSet leftForbidden;
    TInfoUnitConstraintSet rightForbidden;

    if (outputsLeft) {
        leftForbidden.UnionWith(incoming);
    }
    if (outputsRight) {
        rightForbidden.UnionWith(incoming);
    }

    if (outputsLeft && outputsRight) {
        leftForbidden.UnionWith(MakeInfoUnitSet(GetRightInput()->GetOutputIUs()));
        rightForbidden.UnionWith(MakeInfoUnitSet(GetLeftInput()->GetOutputIUs()));
    }

    bool changed = false;
    changed |= PropagateForbidden(GetLeftInput(), leftForbidden);
    changed |= PropagateForbidden(GetRightInput(), rightForbidden);
    return changed;
}

bool TOpUnionAll::PropagateNameConstraints() {
    return false;
}

void ComputePlanNameConstraints(TOpRoot& root) {
    TLogicalNameConstraints().Run(root);
}

const TInfoUnitConstraintSet& GetForbidden(
    IOperator* op)
{
    Y_ENSURE(op);
    return GetComputedNameConstraints(op).GetForbidden();
}

bool ContainsForbidden(const TVector<TInfoUnit>& output, const TInfoUnitConstraintSet& forbidden) {
    for (const auto& iu : output) {
        if (forbidden.contains(iu)) {
            return true;
        }
    }
    return false;
}

} // namespace NKqp
} // namespace NKikimr
