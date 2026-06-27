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

bool AddSharedProducerConstraints(IOperator& consumer);

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
                changed |= AddSharedProducerConstraints(*iter.Current);
            }
            Y_ENSURE(++iteration < 1000, "Name constraint propagation did not converge");
        }
    }
};

} // anonymous namespace

bool TInfoUnitConstraintSet::Add(const TInfoUnit& iu) {
    if (AllExcept_) {
        return Units_.erase(iu) != 0;
    }
    return Units_.insert(iu).second;
}

bool TInfoUnitConstraintSet::Add(const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= Add(iu);
    }
    return changed;
}

bool TInfoUnitConstraintSet::Add(const TInfoUnitConstraintSet& other) {
    if (!other.AllExcept_) {
        return Add(other.Units_);
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

bool TInfoUnitConstraintSet::Remove(const TInfoUnit& iu) {
    if (AllExcept_) {
        return Units_.insert(iu).second;
    }
    return Units_.erase(iu) != 0;
}

bool TInfoUnitConstraintSet::Remove(const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= Remove(iu);
    }
    return changed;
}

bool TInfoUnitConstraintSet::Intersect(const TInfoUnitConstraintSet& other) {
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
    return Forbidden.Add(forbidden);
}

const TInfoUnitConstraintSet& TPlanNameConstraints::GetForbidden() const {
    return Forbidden;
}

namespace {

bool OutputsLeft(const TString& joinKind) {
    return joinKind != "RightOnly" && joinKind != "RightSemi";
}

bool OutputsRight(const TString& joinKind) {
    return joinKind != "LeftOnly" && joinKind != "LeftSemi";
}

TInfoUnitConstraintSet MakeAllNamesConstraint() {
    return TInfoUnitConstraintSet::AllExcept({});
}

TInfoUnitConstraintSet GetMapHiddenInputNames(const TOpMap& map) {
    TInfoUnitConstraintSet result;
    for (const auto& mapElement : map.MapElements) {
        if (mapElement.IsRename()) {
            result.Add(mapElement.GetRename());
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
            if ((childIdx == 0 && OutputsLeft(join->JoinKind)) ||
                (childIdx == 1 && OutputsRight(join->JoinKind)))
            {
                return {};
            }
            return MakeAllNamesConstraint();
        }
        default:
            return {};
    }
}

class THiddenOnAllPaths {
public:
    explicit THiddenOnAllPaths(IOperator* target)
        : Target(target) {
    }

    std::optional<TInfoUnitConstraintSet> Compute(IOperator* op) {
        if (op == Target) {
            return TInfoUnitConstraintSet{};
        }

        if (const auto it = Memo.find(op); it != Memo.end()) {
            return it->second;
        }

        std::optional<TInfoUnitConstraintSet> result;
        for (const auto& [parent, childIdx] : op->Parents) {
            auto parentHidden = Compute(parent);
            if (!parentHidden) {
                continue;
            }

            auto pathHidden = GetHiddenNamesOnEdge(parent, childIdx);
            pathHidden.Add(*parentHidden);
            if (result) {
                result->Intersect(pathHidden);
            } else {
                result = std::move(pathHidden);
            }
        }

        Memo[op] = result;
        return result;
    }

private:
    IOperator* Target;
    THashMap<IOperator*, std::optional<TInfoUnitConstraintSet>> Memo;
};

void CollectProducerSubtree(IOperator* op, THashSet<IOperator*>& result) {
    if (!result.insert(op).second) {
        return;
    }
    for (const auto& child : op->Children) {
        CollectProducerSubtree(child.get(), result);
    }
}

struct TSideBySideInputBranches {
    TIntrusivePtr<IOperator> First;
    TIntrusivePtr<IOperator> Second;
};

std::optional<TSideBySideInputBranches> GetSideBySideInputBranches(IOperator& consumer) {
    if (consumer.Kind == EOperator::Join) {
        auto& join = static_cast<TOpJoin&>(consumer);
        if (OutputsLeft(join.JoinKind) && OutputsRight(join.JoinKind)) {
            return TSideBySideInputBranches{
                join.GetLeftInput(),
                join.GetRightInput()
            };
        }
    }

    return std::nullopt;
}

bool AddSharedProducerConstraints(IOperator& consumer) {
    const auto branches = GetSideBySideInputBranches(consumer);
    if (!branches) {
        return false;
    }

    // If the same producer feeds two side-by-side input branches, a name exposed
    // by that producer would appear through both branches unless one path hides it.
    THashSet<IOperator*> firstProducers;
    THashSet<IOperator*> secondProducers;
    CollectProducerSubtree(branches->First.get(), firstProducers);
    CollectProducerSubtree(branches->Second.get(), secondProducers);

    THiddenOnAllPaths hiddenOnFirstPaths(branches->First.get());
    THiddenOnAllPaths hiddenOnSecondPaths(branches->Second.get());

    bool changed = false;
    for (auto* producer : firstProducers) {
        if (!secondProducers.contains(producer)) {
            continue;
        }

        auto firstHidden = hiddenOnFirstPaths.Compute(producer);
        auto secondHidden = hiddenOnSecondPaths.Compute(producer);
        if (!firstHidden || !secondHidden) {
            continue;
        }

        auto allowed = *firstHidden;
        allowed.Add(*secondHidden);
        changed |= GetMutableComputedNameConstraints(producer).AddForbidden(allowed.Complement());
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
        childForbidden.Add(MakeInfoUnitSet(static_cast<TOpAddDependencies*>(this)->Dependencies));
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
        childForbidden.Remove(iu);
    }
    for (const auto& iu : mapElementOutputs) {
        if (!renameSources.contains(iu)) {
            childForbidden.Add(iu);
        }
    }

    return PropagateForbidden(GetInput(), childForbidden);
}

bool TOpAggregate::PropagateNameConstraints() {
    return false;
}

bool TOpJoin::PropagateNameConstraints() {
    const bool outputsLeft = OutputsLeft(JoinKind);
    const bool outputsRight = OutputsRight(JoinKind);

    const auto incoming = GetForbidden(this);

    TInfoUnitConstraintSet leftForbidden;
    TInfoUnitConstraintSet rightForbidden;

    if (outputsLeft) {
        leftForbidden.Add(incoming);
    }
    if (outputsRight) {
        rightForbidden.Add(incoming);
    }

    if (outputsLeft && outputsRight) {
        leftForbidden.Add(MakeInfoUnitSet(GetRightInput()->GetOutputIUs()));
        rightForbidden.Add(MakeInfoUnitSet(GetLeftInput()->GetOutputIUs()));
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

TInfoUnitConstraintSet GetForbidden(
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
