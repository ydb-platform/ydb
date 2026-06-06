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

void AddPreferredAliasRename(
    TRenameMap& renameMap,
    const TPlanProps& props,
    const TIntrusivePtr<IOperator>& aliasesAt,
    const TInfoUnit& iu,
    const TInfoUnitSet& liveOut)
{
    const auto* candidates = props.Aliases.GetAliases(aliasesAt.get(), iu);
    if (!candidates) {
        return;
    }

    const auto preferred = ChoosePreferredAlias(*candidates, liveOut);
    if (!preferred || *preferred == iu || !ContainsInfoUnit(aliasesAt->GetOutputIUs(), *preferred)) {
        return;
    }

    const auto [it, inserted] = renameMap.emplace(iu, *preferred);
    if (!inserted && it->second != *preferred) {
        renameMap.erase(it);
    }
}

TRenameMap BuildPreferredAliasRenameMap(
    const TPlanProps& props,
    const TIntrusivePtr<IOperator>& aliasesAt,
    const TVector<TInfoUnit>& usedIUs,
    const TInfoUnitSet& liveOut)
{
    TRenameMap renameMap;
    for (const auto& iu : usedIUs) {
        AddPreferredAliasRename(renameMap, props, aliasesAt, iu, liveOut);
    }
    return renameMap;
}

bool RenameInfoUnit(TInfoUnit& iu, const TRenameMap& renameMap) {
    const auto it = renameMap.find(iu);
    if (it == renameMap.end()) {
        return false;
    }

    iu = it->second;
    return true;
}

bool HasDirectExpressionRename(const TExpression& expr, const TRenameMap& renameMap) {
    for (const auto& iu : expr.GetInputIUs(true, false)) {
        if (renameMap.contains(iu)) {
            return true;
        }
    }
    return false;
}

bool RenameExpression(TExpression& expr, const TRenameMap& renameMap) {
    if (renameMap.empty() || !HasDirectExpressionRename(expr, renameMap)) {
        return false;
    }

    expr = expr.ApplyRenames(renameMap);
    return true;
}

TVector<TInfoUnit> GetMapInputIUs(const TOpMap& map) {
    TVector<TInfoUnit> usedIUs;
    for (const auto& mapElement : map.MapElements) {
        if (mapElement.IsRename()) {
            continue;
        }
        const auto expressionIUs = mapElement.GetExpression().GetInputIUs(false, true);
        usedIUs.insert(usedIUs.end(), expressionIUs.begin(), expressionIUs.end());
    }
    return usedIUs;
}

bool RewriteMapInputs(TOpMap& map, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    const auto renameMap = BuildPreferredAliasRenameMap(props, map.GetInput(), GetMapInputIUs(map), liveOut);
    if (renameMap.empty()) {
        return false;
    }

    bool changed = false;
    TRenameMap mapRenameMap;
    for (auto& mapElement : map.MapElements) {
        if (mapElement.IsRename()) {
            continue;
        }

        TRenameMap elementRenameMap;
        for (const auto& [from, to] : renameMap) {
            if (to == mapElement.GetElementName()) {
                continue;
            }
            elementRenameMap.emplace(from, to);
            mapRenameMap.emplace(from, to);
        }

        changed |= RenameExpression(mapElement.GetExpressionRef(), elementRenameMap);
    }
    const bool subplansChanged = props.Subplans.RenameIUs(mapRenameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

bool RewriteFilterInputs(TOpFilter& filter, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    // Source-adjacent filters are the contract surface for range and OLAP predicate pushdown.
    if (filter.GetInput()->Kind == EOperator::Source) {
        return false;
    }

    const auto renameMap = BuildPreferredAliasRenameMap(props, filter.GetInput(), filter.FilterExpr.GetInputIUs(false, true), liveOut);
    if (renameMap.empty()) {
        return false;
    }

    bool changed = RenameExpression(filter.FilterExpr, renameMap);
    const bool subplansChanged = props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

void MergeRenameMap(TRenameMap& target, const TRenameMap& source) {
    for (const auto& [from, to] : source) {
        const auto [it, inserted] = target.emplace(from, to);
        if (!inserted && it->second != to) {
            target.erase(it);
        }
    }
}

bool RewriteJoinInputs(TOpJoin& join, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    TVector<TInfoUnit> leftUsed;
    TVector<TInfoUnit> rightUsed;
    TVector<TInfoUnit> filterUsed;

    for (const auto& [leftKey, rightKey] : join.JoinKeys) {
        leftUsed.push_back(leftKey);
        rightUsed.push_back(rightKey);
    }
    for (const auto& filter : join.JoinFilters) {
        const auto filterIUs = filter.GetInputIUs(false, true);
        filterUsed.insert(filterUsed.end(), filterIUs.begin(), filterIUs.end());
    }

    const auto leftRenameMap = BuildPreferredAliasRenameMap(props, join.GetLeftInput(), leftUsed, liveOut);
    const auto rightRenameMap = BuildPreferredAliasRenameMap(props, join.GetRightInput(), rightUsed, liveOut);

    TRenameMap filterRenameMap = BuildPreferredAliasRenameMap(props, join.GetLeftInput(), filterUsed, liveOut);
    MergeRenameMap(filterRenameMap, BuildPreferredAliasRenameMap(props, join.GetRightInput(), filterUsed, liveOut));

    bool changed = false;
    for (auto& [leftKey, rightKey] : join.JoinKeys) {
        changed |= RenameInfoUnit(leftKey, leftRenameMap);
        changed |= RenameInfoUnit(rightKey, rightRenameMap);
    }
    for (auto& filter : join.JoinFilters) {
        changed |= RenameExpression(filter, filterRenameMap);
    }

    TRenameMap subplanRenameMap = leftRenameMap;
    MergeRenameMap(subplanRenameMap, rightRenameMap);
    MergeRenameMap(subplanRenameMap, filterRenameMap);
    const bool subplansChanged = props.Subplans.RenameIUs(subplanRenameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

bool RewriteLimitInputs(TOpLimit& limit, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    TVector<TInfoUnit> usedIUs = limit.LimitCond.GetInputIUs(false, true);
    if (const auto offset = limit.GetOffsetCond()) {
        const auto offsetIUs = offset->GetInputIUs(false, true);
        usedIUs.insert(usedIUs.end(), offsetIUs.begin(), offsetIUs.end());
    }

    const auto renameMap = BuildPreferredAliasRenameMap(props, limit.GetInput(), usedIUs, liveOut);
    if (renameMap.empty()) {
        return false;
    }

    bool changed = HasDirectExpressionRename(limit.LimitCond, renameMap);
    if (const auto offset = limit.GetOffsetCond()) {
        changed |= HasDirectExpressionRename(*offset, renameMap);
    }
    if (changed) {
        limit.RenameIUs(renameMap, ctx.ExprCtx);
    }
    const bool subplansChanged = props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

bool RewriteSortInputs(TOpSort& sort, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    TVector<TInfoUnit> usedIUs;
    for (const auto& sortElement : sort.SortElements) {
        usedIUs.push_back(sortElement.SortColumn);
    }
    if (sort.LimitCond) {
        const auto limitIUs = sort.LimitCond->GetInputIUs(false, true);
        usedIUs.insert(usedIUs.end(), limitIUs.begin(), limitIUs.end());
    }

    const auto renameMap = BuildPreferredAliasRenameMap(props, sort.GetInput(), usedIUs, liveOut);
    if (renameMap.empty()) {
        return false;
    }

    bool changed = false;
    for (auto& sortElement : sort.SortElements) {
        changed |= RenameInfoUnit(sortElement.SortColumn, renameMap);
    }
    if (sort.LimitCond) {
        changed |= RenameExpression(*sort.LimitCond, renameMap);
    }
    const bool subplansChanged = props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

bool RewriteAggregateInputs(TOpAggregate& aggregate, const TInfoUnitSet& liveOut, TRBOContext& ctx, TPlanProps& props) {
    TVector<TInfoUnit> usedIUs = aggregate.KeyColumns;
    for (const auto& traits : aggregate.AggregationTraitsList) {
        usedIUs.push_back(traits.OriginalColName);
    }

    const auto renameMap = BuildPreferredAliasRenameMap(props, aggregate.GetInput(), usedIUs, liveOut);
    if (renameMap.empty()) {
        return false;
    }

    const auto oldKeys = aggregate.KeyColumns;
    const auto oldTraits = aggregate.AggregationTraitsList;

    bool changed = false;
    for (auto& key : aggregate.KeyColumns) {
        changed |= RenameInfoUnit(key, renameMap);
    }
    for (auto& traits : aggregate.AggregationTraitsList) {
        changed |= RenameInfoUnit(traits.OriginalColName, renameMap);
        if (aggregate.IsDistinctAll()) {
            changed |= RenameInfoUnit(traits.ResultColName, renameMap);
        }
    }

    if (HasOutputConflicts(aggregate.GetOutputIUs()) || !CanExposeToParents(&aggregate, props)) {
        aggregate.KeyColumns = oldKeys;
        aggregate.AggregationTraitsList = oldTraits;
        return false;
    }

    const bool subplansChanged = props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return changed || subplansChanged;
}

} // anonymous namespace

bool TRewriteExpressionsToPreferredAliasesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    const auto liveIt = props.LiveOut.find(input.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    switch (input->Kind) {
        case EOperator::Map:
            return RewriteMapInputs(*CastOperator<TOpMap>(input), liveIt->second, ctx, props);
        case EOperator::Filter:
            return RewriteFilterInputs(*CastOperator<TOpFilter>(input), liveIt->second, ctx, props);
        case EOperator::Join:
            return RewriteJoinInputs(*CastOperator<TOpJoin>(input), liveIt->second, ctx, props);
        case EOperator::Aggregate:
            return RewriteAggregateInputs(*CastOperator<TOpAggregate>(input), liveIt->second, ctx, props);
        case EOperator::Limit:
            return RewriteLimitInputs(*CastOperator<TOpLimit>(input), liveIt->second, ctx, props);
        case EOperator::Sort:
            return RewriteSortInputs(*CastOperator<TOpSort>(input), liveIt->second, ctx, props);
        default:
            return false;
    }
}

} // namespace NKqp
} // namespace NKikimr
