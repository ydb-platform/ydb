#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

using TCandidates = TPlanAliases::TCandidates;
using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

// Pinned names are forced to stay alive by contracts this rule can never
// rewrite: root output names (hard), aggregate keys and UnionAll columns
// (soft; only their dedicated push rules may rename them). An unpinned class
// member can always lose all its uses and fold into its producer, so uses
// must converge onto the pinned name — converging onto an unpinned one keeps
// two names of the same class alive forever.
enum class EPinRank : ui8 {
    Hard = 0,
    Soft = 1,
    None = 2,
};

// Hard pins apply only in the root alias region: below an alias-class cut a
// coinciding name is a different incarnation of the class and root does not
// consume it directly.
EPinRank GetPinRank(const TPinnedNames& pinned, const TInfoUnit& iu, bool inRootRegion) {
    if (inRootRegion && pinned.Hard.contains(iu)) {
        return EPinRank::Hard;
    }
    if (pinned.Soft.contains(iu)) {
        return EPinRank::Soft;
    }
    return EPinRank::None;
}

// Preference order inside a class: hard pins, then soft pins, then free names.
//
// Among free names prefer the oldest: it is defined lowest in the plan, so its
// scope dominates every use site and all uses can converge to it, letting the
// newer aliases lose their uses and get pruned. It is also stable under
// rewrites (appending new aliases never changes which candidate is oldest),
// which makes the rewrite terminate.
//
// Among soft pins prefer the newest: alias classes are cut at aggregates and
// unions, so within one class a newer soft pin belongs to the consumer that
// terminates the region above (an aggregate key), while an older one belongs
// to the producer that starts it below (a UnionAll column). Converging uses
// onto the consumer's name lets the producer-side binding fold downward; the
// reverse leaves both names alive.
bool IsBetterCandidate(const TAliasCandidate& candidate, const TAliasCandidate& best, const TPinnedNames& pinned, bool inRootRegion) {
    const auto candidateRank = GetPinRank(pinned, candidate.IU, inRootRegion);
    const auto bestRank = GetPinRank(pinned, best.IU, inRootRegion);
    if (candidateRank != bestRank) {
        return candidateRank < bestRank;
    }

    if (candidate.Priority != best.Priority) {
        if (candidateRank == EPinRank::Soft) {
            return candidate.Priority > best.Priority;
        }
        return candidate.Priority < best.Priority;
    }

    return candidate.IU.GetFullName() < best.IU.GetFullName();
}

std::optional<TInfoUnit> ChoosePreferredAlias(const TCandidates& candidates, const TPinnedNames& pinned, bool inRootRegion) {
    const TAliasCandidate* best = nullptr;
    for (const auto& candidate : candidates) {
        if (IsGeneratedIgnoreIU(candidate.IU)) {
            continue;
        }

        if (!best || IsBetterCandidate(candidate, *best, pinned, inRootRegion)) {
            best = &candidate;
        }
    }

    return best ? std::optional<TInfoUnit>(best->IU) : std::nullopt;
}

TIntrusivePtr<IOperator> FindUsedIUOwner(IOperator& op, const TInfoUnit& iu) {
    TIntrusivePtr<IOperator> owner;
    for (const auto& child : op.GetChildren()) {
        if (!ContainsInfoUnit(child->GetOutputIUs(), iu)) {
            continue;
        }

        if (owner) {
            return {};
        }
        owner = child;
    }
    return owner;
}

bool CanRenameUsedIUTo(IOperator& op, const TInfoUnit& from, const TInfoUnit& to) {
    if (op.Kind != EOperator::Map) {
        return true;
    }

    const auto& map = static_cast<const TOpMap&>(op);
    for (const auto& mapElement : map.MapElements) {
        if (mapElement.IsRename() || mapElement.GetElementName() != to) {
            continue;
        }

        const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
        if (ContainsInfoUnit(usedIUs, from)) {
            return false;
        }
    }
    return true;
}

bool ContainsAliasCandidate(const TCandidates& candidates, const TInfoUnit& iu) {
    for (const auto& candidate : candidates) {
        if (candidate.IU == iu) {
            return true;
        }
    }
    return false;
}

bool IsRedundantAliasAppend(TOpMap& map, const TMapElement& mapElement) {
    if (!mapElement.IsColumnAccess() || !ContainsInfoUnit(map.GetInput()->GetOutputIUs(), mapElement.GetElementName())) {
        return false;
    }

    if (mapElement.GetColumnAccess() == mapElement.GetElementName()) {
        return true;
    }

    const auto* candidates = GetAliases(map.GetInput().get(), mapElement.GetColumnAccess());
    return candidates && ContainsAliasCandidate(*candidates, mapElement.GetElementName());
}

bool DropRedundantAliasAppends(IOperator& op) {
    if (op.Kind != EOperator::Map) {
        return false;
    }

    auto& map = static_cast<TOpMap&>(op);
    TVector<TMapElement> elements;
    elements.reserve(map.MapElements.size());

    bool changed = false;
    for (auto mapElement : map.MapElements) {
        if (IsRedundantAliasAppend(map, mapElement)) {
            changed = true;
            continue;
        }
        elements.push_back(std::move(mapElement));
    }

    if (changed) {
        map.MapElements = std::move(elements);
    }
    return changed;
}

void AddPreferredAliasRename(
    TRenameMap& renameMap,
    IOperator& op,
    const TIntrusivePtr<IOperator>& aliasesAt,
    const TInfoUnit& iu,
    const TPinnedNames& pinned)
{
    const auto* candidates = GetAliases(aliasesAt.get(), iu);
    if (!candidates) {
        return;
    }

    const auto preferred = ChoosePreferredAlias(*candidates, pinned, aliasesAt->Props.Analysis.InRootAliasRegion);
    if (!preferred || *preferred == iu || !ContainsInfoUnit(aliasesAt->GetOutputIUs(), *preferred)) {
        return;
    }
    if (!CanRenameUsedIUTo(op, iu, *preferred)) {
        return;
    }

    const auto [it, inserted] = renameMap.emplace(iu, *preferred);
    if (!inserted && it->second != *preferred) {
        renameMap.erase(it);
    }
}

TRenameMap BuildPreferredAliasRenameMap(IOperator& op, const TVector<TInfoUnit>& usedIUs, const TPinnedNames& pinned) {
    TRenameMap renameMap;
    for (const auto& iu : usedIUs) {
        auto aliasesAt = FindUsedIUOwner(op, iu);
        if (!aliasesAt) {
            continue;
        }

        AddPreferredAliasRename(renameMap, op, aliasesAt, iu, pinned);
    }
    return renameMap;
}

} // anonymous namespace

bool TRewriteExpressionsToPreferredAliasesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    const bool droppedRedundantAppends = DropRedundantAliasAppends(*input);

    const auto usedIUs = input->GetUsedIUs(props);
    if (usedIUs.empty()) {
        return droppedRedundantAppends;
    }

    Y_ENSURE(props.PinnedNames.has_value(), "Pinned names requested before plan aliases were computed");
    const auto renameMap = BuildPreferredAliasRenameMap(*input, usedIUs, *props.PinnedNames);
    if (renameMap.empty()) {
        return droppedRedundantAppends;
    }

    input->RenameUsedIUs(renameMap, ctx.ExprCtx);
    const bool subplansChanged = props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);
    Y_UNUSED(subplansChanged);
    return true;
}

} // namespace NKqp
} // namespace NKikimr
