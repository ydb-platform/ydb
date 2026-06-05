#include "kqp_rules_include.h"

#include <algorithm>
#include <functional>

namespace NKikimr {
namespace NKqp {

namespace {

using TAliasMap = TPlanAliases::TAliasMap;
using TCandidates = TPlanAliases::TCandidates;

bool ContainsInfoUnit(const TVector<TInfoUnit>& ius, const TInfoUnit& iu) {
    return std::find(ius.begin(), ius.end(), iu) != ius.end();
}

TInfoUnitSet MakeInfoUnitSetLocal(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    for (const auto& iu : ius) {
        result.insert(iu);
    }
    return result;
}

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

TAliasMap RestrictAliases(const TAliasMap& inputAliases, const TVector<TInfoUnit>& output) {
    TAliasMap aliases;
    const auto visible = MakeInfoUnitSetLocal(output);
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

TAliasMap BuildMapAliases(const TIntrusivePtr<TOpMap>& map, const TPlanAliases& planAliases) {
    const auto input = map->GetInput();
    const auto inputOutput = input->GetOutputIUs();
    const auto output = map->GetOutputIUs();
    const auto visible = MakeInfoUnitSetLocal(output);
    const auto opIt = planAliases.AliasesAtOutput.find(input.get());
    const auto inputAliases = opIt == planAliases.AliasesAtOutput.end() ? BuildIdentityAliases(inputOutput) : opIt->second;

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

    for (const auto& mapElement : map->MapElements) {
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

TAliasMap BuildJoinAliases(const TIntrusivePtr<TOpJoin>& join, const TPlanAliases& planAliases) {
    TAliasMap aliases;
    const auto output = join->GetOutputIUs();
    const auto visible = MakeInfoUnitSetLocal(output);

    auto addChildAliases = [&](const TIntrusivePtr<IOperator>& child) {
        const auto opIt = planAliases.AliasesAtOutput.find(child.get());
        const auto childOutput = child->GetOutputIUs();
        const auto childAliases = opIt == planAliases.AliasesAtOutput.end() ? BuildIdentityAliases(childOutput) : opIt->second;

        TVector<TInfoUnit> childVisibleOutput;
        const auto childVisible = MakeInfoUnitSetLocal(childOutput);
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

    addChildAliases(join->GetLeftInput());
    addChildAliases(join->GetRightInput());
    return aliases;
}

TAliasMap BuildAliasesForOperator(const TIntrusivePtr<IOperator>& op, const TPlanAliases& planAliases) {
    switch (op->Kind) {
        case EOperator::Map:
            return BuildMapAliases(CastOperator<TOpMap>(op), planAliases);
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::AddDependencies: {
            const auto input = CastOperator<IUnaryOperator>(op)->GetInput();
            const auto opIt = planAliases.AliasesAtOutput.find(input.get());
            const auto inputAliases = opIt == planAliases.AliasesAtOutput.end() ? BuildIdentityAliases(input->GetOutputIUs()) : opIt->second;
            return RestrictAliases(inputAliases, op->GetOutputIUs());
        }
        case EOperator::Join:
            return BuildJoinAliases(CastOperator<TOpJoin>(op), planAliases);
        default:
            return BuildIdentityAliases(op->GetOutputIUs());
    }
}

std::optional<TInfoUnit> ChoosePreferredAlias(const TCandidates& candidates, const TInfoUnitSet& liveOut) {
    if (candidates.empty()) {
        return std::nullopt;
    }

    const TAliasCandidate* bestLive = nullptr;
    const TAliasCandidate* bestBase = &candidates.front();
    for (const auto& candidate : candidates) {
        if (liveOut.contains(candidate.IU) &&
            (!bestLive || candidate.Priority > bestLive->Priority ||
                (candidate.Priority == bestLive->Priority && candidate.IU.GetFullName() < bestLive->IU.GetFullName()))) {
            bestLive = &candidate;
        }
        if (candidate.Priority < bestBase->Priority ||
            (candidate.Priority == bestBase->Priority && candidate.IU.GetFullName() < bestBase->IU.GetFullName())) {
            bestBase = &candidate;
        }
    }

    return bestLive ? bestLive->IU : bestBase->IU;
}

} // anonymous namespace

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

        root.PlanProps.Aliases.AliasesAtOutput[op.get()] = BuildAliasesForOperator(op, root.PlanProps.Aliases);
    };

    compute(root.GetInput());
    for (const auto& subPlan : root.PlanProps.Subplans.Get()) {
        compute(CastOperator<IOperator>(subPlan.Plan));
    }
}

bool TRewriteExpressionsToPreferredAliasesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    const auto liveIt = props.LiveOut.find(filter.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap;
    for (const auto& iu : filter->FilterExpr.GetInputIUs(false, true)) {
        const auto* candidates = props.Aliases.GetAliases(filter->GetInput().get(), iu);
        if (!candidates) {
            continue;
        }

        const auto preferred = ChoosePreferredAlias(*candidates, liveIt->second);
        if (preferred && *preferred != iu && ContainsInfoUnit(filter->GetInput()->GetOutputIUs(), *preferred)) {
            renameMap[iu] = *preferred;
        }
    }

    if (renameMap.empty()) {
        return false;
    }

    filter->RenameIUs(renameMap, ctx.ExprCtx);
    props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return true;
}

} // namespace NKqp
} // namespace NKikimr
