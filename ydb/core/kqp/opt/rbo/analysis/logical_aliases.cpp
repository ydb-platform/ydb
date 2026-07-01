#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <algorithm>
#include <functional>

namespace NKikimr {
namespace NKqp {

namespace {

using TAliasMap = TPlanAliases::TAliasMap;
using TCandidates = TPlanAliases::TCandidates;

const TCandidates* FindAliases(const TAliasMap& aliases, const TInfoUnit& iu) {
    const auto it = aliases.find(iu);
    return it == aliases.end() ? nullptr : &it->second;
}

i32 GetAliasPriority(const TCandidates& candidates, const TInfoUnit& iu) {
    for (const auto& candidate : candidates) {
        if (candidate.IU == iu) {
            return candidate.Priority;
        }
    }
    return 0;
}

TInfoUnit GetCanonicalAlias(const TCandidates& candidates) {
    Y_ENSURE(!candidates.empty());
    const auto* best = &candidates.front();
    for (const auto& candidate : candidates) {
        if (candidate.Priority < best->Priority ||
            (candidate.Priority == best->Priority && candidate.IU.GetFullName() < best->IU.GetFullName())) {
            best = &candidate;
        }
    }
    return best->IU;
}

void AddCandidate(TCandidates& candidates, const TAliasCandidate& candidate) {
    for (auto& existing : candidates) {
        if (existing.IU == candidate.IU) {
            existing.Priority = std::max(existing.Priority, candidate.Priority);
            return;
        }
    }
    candidates.push_back(candidate);
}

TCandidates RestrictCandidates(const TCandidates& candidates, const TInfoUnitSet& visible) {
    TCandidates result;
    result.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        if (visible.contains(candidate.IU)) {
            AddCandidate(result, candidate);
        }
    }
    return result;
}

void AddAliasClass(TAliasMap& aliases, const TCandidates& candidates) {
    if (candidates.empty()) {
        return;
    }

    for (const auto& candidate : candidates) {
        aliases[candidate.IU] = candidates;
    }
}

TAliasMap BuildIdentityAliases(const TVector<TInfoUnit>& output) {
    TAliasMap aliases;
    for (const auto& iu : output) {
        AddAliasClass(aliases, TCandidates{{iu, 0}});
    }
    return aliases;
}

TAliasMap GetAliasesAtOutput(const TPlanAliases& planAliases, const TIntrusivePtr<IOperator>& op) {
    const auto it = planAliases.AliasesAtOutput.find(op.get());
    return it == planAliases.AliasesAtOutput.end() ? BuildIdentityAliases(op->GetOutputIUs()) : it->second;
}

TAliasMap RestrictAliases(const TAliasMap& inputAliases, const TVector<TInfoUnit>& output) {
    TAliasMap aliases;
    const auto visible = MakeInfoUnitSet(output);
    TInfoUnitSet addedCanonicals;

    for (const auto& iu : output) {
        const auto* candidates = FindAliases(inputAliases, iu);
        if (!candidates) {
            AddAliasClass(aliases, TCandidates{{iu, 0}});
            continue;
        }

        const auto canonical = GetCanonicalAlias(*candidates);
        if (!addedCanonicals.insert(canonical).second) {
            continue;
        }

        auto restricted = RestrictCandidates(*candidates, visible);
        if (restricted.empty()) {
            restricted.push_back({iu, GetAliasPriority(*candidates, iu)});
        }
        AddAliasClass(aliases, restricted);
    }

    return aliases;
}

TAliasMap BuildPassthroughAliases(IUnaryOperator& op, const TPlanAliases& planAliases) {
    const auto input = op.GetInput();
    return RestrictAliases(GetAliasesAtOutput(planAliases, input), op.GetOutputIUs());
}

TAliasMap BuildMapAliases(TOpMap& map, const TPlanAliases& planAliases) {
    const auto input = map.GetInput();
    const auto inputOutput = input->GetOutputIUs();
    const auto output = map.GetOutputIUs();
    const auto visible = MakeInfoUnitSet(output);
    const auto inputAliases = GetAliasesAtOutput(planAliases, input);

    THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction> classByCanonical;
    TVector<TCandidates> classes;

    for (const auto& iu : inputOutput) {
        if (!visible.contains(iu)) {
            continue;
        }

        const auto* candidates = FindAliases(inputAliases, iu);
        if (!candidates) {
            continue;
        }

        const auto canonical = GetCanonicalAlias(*candidates);
        if (!classByCanonical.contains(canonical)) {
            classByCanonical[canonical] = classes.size();
            classes.push_back(RestrictCandidates(*candidates, visible));
        }
    }

    for (const auto& mapElement : map.MapElements) {
        const auto to = mapElement.GetElementName();
        if (!visible.contains(to)) {
            continue;
        }

        if (!mapElement.IsColumnAccess()) {
            classes.push_back(TCandidates{{to, 0}});
            continue;
        }

        const auto from = mapElement.GetColumnAccess();
        const auto* sourceCandidates = FindAliases(inputAliases, from);
        TCandidates sourceClass = sourceCandidates ? *sourceCandidates : TCandidates{{from, 0}};
        const auto canonical = sourceCandidates ? GetCanonicalAlias(*sourceCandidates) : from;
        const auto fromPriority = GetAliasPriority(sourceClass, from);

        size_t classIdx = 0;
        const auto classIt = classByCanonical.find(canonical);
        if (classIt == classByCanonical.end()) {
            classIdx = classes.size();
            classByCanonical[canonical] = classIdx;
            classes.push_back(RestrictCandidates(sourceClass, visible));
        } else {
            classIdx = classIt->second;
        }

        AddCandidate(classes[classIdx], {to, fromPriority + 1});
    }

    TAliasMap aliases;
    for (const auto& aliasClass : classes) {
        AddAliasClass(aliases, aliasClass);
    }

    for (const auto& iu : output) {
        if (!FindAliases(aliases, iu)) {
            AddAliasClass(aliases, TCandidates{{iu, 0}});
        }
    }

    return aliases;
}

TAliasMap BuildJoinAliases(TOpJoin& join, const TPlanAliases& planAliases) {
    TAliasMap aliases;
    const auto output = join.GetOutputIUs();
    const auto visible = MakeInfoUnitSet(output);

    auto addChildAliases = [&](const TIntrusivePtr<IOperator>& child) {
        const auto childOutput = child->GetOutputIUs();
        const auto childAliases = GetAliasesAtOutput(planAliases, child);

        TVector<TInfoUnit> childVisibleOutput;
        const auto childVisible = MakeInfoUnitSet(childOutput);
        for (const auto& iu : output) {
            if (childVisible.contains(iu)) {
                childVisibleOutput.push_back(iu);
            }
        }

        const auto restricted = RestrictAliases(childAliases, childVisibleOutput);
        for (const auto& [iu, candidates] : restricted) {
            if (visible.contains(iu)) {
                aliases[iu] = candidates;
            }
        }
    };

    addChildAliases(join.GetLeftInput());
    addChildAliases(join.GetRightInput());
    return aliases;
}

} // anonymous namespace

TPlanAliases::TAliasMap IOperator::ComputeAliases(const TPlanAliases& planAliases) {
    Y_UNUSED(planAliases);
    return BuildIdentityAliases(GetOutputIUs());
}

TPlanAliases::TAliasMap TOpMap::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildMapAliases(*this, planAliases);
}

TPlanAliases::TAliasMap TOpAddDependencies::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildPassthroughAliases(*this, planAliases);
}

TPlanAliases::TAliasMap TOpFilter::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildPassthroughAliases(*this, planAliases);
}

TPlanAliases::TAliasMap TOpJoin::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildJoinAliases(*this, planAliases);
}

TPlanAliases::TAliasMap TOpLimit::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildPassthroughAliases(*this, planAliases);
}

TPlanAliases::TAliasMap TOpSort::ComputeAliases(const TPlanAliases& planAliases) {
    return BuildPassthroughAliases(*this, planAliases);
}

void ComputePlanAliases(TOpRoot& root) {
    root.PlanProps.Aliases.Clear();

    THashSet<IOperator*> visited;
    std::function<void(const TIntrusivePtr<IOperator>&)> compute = [&](const TIntrusivePtr<IOperator>& op) {
        if (!op || !visited.insert(op.get()).second) {
            return;
        }

        for (const auto& child : op->Children) {
            compute(child);
        }

        root.PlanProps.Aliases.AliasesAtOutput[op.get()] = op->ComputeAliases(root.PlanProps.Aliases);
    };

    compute(root.GetInput());
    for (const auto& subPlan : root.PlanProps.Subplans.Get()) {
        compute(CastOperator<IOperator>(subPlan.Plan));
    }
}

} // namespace NKqp
} // namespace NKikimr
