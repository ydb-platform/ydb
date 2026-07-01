#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

using TCandidates = TPlanAliases::TCandidates;
using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

std::optional<TInfoUnit> ChoosePreferredAlias(const TCandidates& candidates, const TInfoUnitSet& liveOut) {
    if (candidates.empty()) {
        return std::nullopt;
    }

    const TAliasCandidate* bestLive = nullptr;
    const TAliasCandidate* bestGeneratedLive = nullptr;
    const TAliasCandidate* bestBase = nullptr;
    const TAliasCandidate* bestGeneratedBase = nullptr;
    for (const auto& candidate : candidates) {
        const bool isGenerated = IsGeneratedIgnoreIU(candidate.IU);
        if (liveOut.contains(candidate.IU)) {
            const auto*& target = isGenerated ? bestGeneratedLive : bestLive;
            if (!target || candidate.Priority > target->Priority ||
                (candidate.Priority == target->Priority && candidate.IU.GetFullName() < target->IU.GetFullName())) {
                target = &candidate;
            }
        }

        const auto*& targetBase = isGenerated ? bestGeneratedBase : bestBase;
        if (!targetBase || candidate.Priority < targetBase->Priority ||
            (candidate.Priority == targetBase->Priority && candidate.IU.GetFullName() < targetBase->IU.GetFullName())) {
            targetBase = &candidate;
        }
    }

    if (bestLive) {
        return bestLive->IU;
    }
    if (bestBase) {
        return bestBase->IU;
    }
    if (bestGeneratedLive) {
        return bestGeneratedLive->IU;
    }
    return bestGeneratedBase ? std::optional<TInfoUnit>(bestGeneratedBase->IU) : std::nullopt;
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
    const TInfoUnitSet& liveOut)
{
    const auto* candidates = GetAliases(aliasesAt.get(), iu);
    if (!candidates) {
        return;
    }

    const auto preferred = ChoosePreferredAlias(*candidates, liveOut);
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

TRenameMap BuildPreferredAliasRenameMap(IOperator& op, const TVector<TInfoUnit>& usedIUs, const TInfoUnitSet& liveOut) {
    TRenameMap renameMap;
    for (const auto& iu : usedIUs) {
        auto aliasesAt = FindUsedIUOwner(op, iu);
        if (!aliasesAt) {
            continue;
        }

        AddPreferredAliasRename(renameMap, op, aliasesAt, iu, liveOut);
    }
    return renameMap;
}

bool ShouldSkipPreferredAliasRewrite(const IOperator& op) {
    // Source-adjacent filters are the contract surface for range and OLAP predicate pushdown.
    return op.Kind == EOperator::Filter &&
        op.Children.size() == 1 &&
        op.Children.front()->Kind == EOperator::Source;
}

} // anonymous namespace

bool TRewriteExpressionsToPreferredAliasesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (ShouldSkipPreferredAliasRewrite(*input)) {
        return false;
    }

    const bool droppedRedundantAppends = DropRedundantAliasAppends(*input);

    const auto usedIUs = input->GetUsedIUs(props);
    if (usedIUs.empty()) {
        return droppedRedundantAppends;
    }

    const auto renameMap = BuildPreferredAliasRenameMap(*input, usedIUs, GetLiveOut(input.get()));
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
