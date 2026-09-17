#include "dependent_join_pushdown.h"

#include "../kqp_rules_include.h"
#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace NKikimr {
namespace NKqp {

namespace {

// Some helpers.
bool ColumnsIntersect(const TVector<TInfoUnit>& left, const TVector<TInfoUnit>& right) {
    for (const auto& iu : left) {
        if (ContainsInfoUnit(right, iu)) {
            return true;
        }
    }
    return false;
}

bool HasOperatorBelow(const TIntrusivePtr<IOperator>& op, EOperator kind) {
    if (op->Kind == kind) {
        return true;
    }
    for (const auto& child : op->Children) {
        if (HasOperatorBelow(child, kind)) {
            return true;
        }
    }
    return false;
}

TIntrusivePtr<TOpDependentJoin> PushInto(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<IOperator>& newInput) {
    return MakeIntrusive<TOpDependentJoin>(dependentJoin->GetDomain(), newInput, dependentJoin->Dependencies, dependentJoin->Pos);
}

TIntrusivePtr<IOperator> MakeCrossJoinWithDomain(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<IOperator>& input) {
    return MakeIntrusive<TOpJoin>(dependentJoin->GetDomain(), input, dependentJoin->Pos, "Cross", TVector<std::pair<TInfoUnit, TInfoUnit>>{});
}

TVector<TInfoUnit> MissingDomainColumns(const TVector<TInfoUnit>& dependencies, const TVector<TInfoUnit>& present) {
    TVector<TInfoUnit> result;
    for (const auto& iu : dependencies) {
        if (!ContainsInfoUnit(present, iu)) {
            result.push_back(iu);
        }
    }
    return result;
}

// Here is a special case for count(*). count(*) with empty keys returns 0 on empty input, but with group by keys we can lost those values.
// So, we make left join to restore columns and apply coalesce (column, 0).
TIntrusivePtr<IOperator> RestoreEmptyGroupCounts(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<TOpAggregate>& aggregate,
                                                 const TVector<TInfoUnit>& countResults, TRBOContext& ctx, TPlanProps& props) {
    const auto& dependencies = dependentJoin->Dependencies;
    const auto pos = aggregate->Pos;

    TIntrusivePtr<IOperator> leftInput = dependentJoin->GetDomain();
    TIntrusivePtr<IOperator> rightInput = aggregate;

    TInfoUnitSet usedIUs;
    NMapRenames::AddUsedIUs(usedIUs, leftInput->GetOutputIUs());
    NMapRenames::AddUsedIUs(usedIUs, rightInput->GetOutputIUs());

    TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
    for (const auto& iu : dependencies) {
        joinKeys.emplace_back(iu, iu);
    }
    joinKeys = MakeNullSafeJoinKeys(leftInput, rightInput, joinKeys, pos, ctx, props, usedIUs);

    NMapRenames::TRenameMap rightRenames;
    TVector<std::pair<TInfoUnit, TInfoUnit>> renamedCounts;
    for (const auto& iu : dependencies) {
        rightRenames.emplace(iu, NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs));
    }
    for (const auto& iu : countResults) {
        if (rightRenames.contains(iu)) {
            continue;
        }
        auto renamedIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
        rightRenames.emplace(iu, renamedIU);
        renamedCounts.emplace_back(iu, renamedIU);
    }

    auto join = NMapRenames::MakeJoinWithRightRenames(leftInput, rightInput, pos, "Left", joinKeys, {}, rightRenames, ctx.ExprCtx, props);

    TVector<TMapElement> resultElements;
    resultElements.reserve(renamedCounts.size());
    for (const auto& [resultIU, renamedIU] : renamedCounts) {
        resultElements.emplace_back(
            resultIU, MakeBinaryPredicate("Coalesce", MakeColumnAccess(renamedIU, pos, &ctx.ExprCtx, &props), MakeConstant("Uint64", "0", pos, &ctx.ExprCtx)));
    }

    return MakeIntrusive<TOpMap>(join, pos, resultElements);
}

} // anonymous namespace


// Domain projection is a distinct on free variables.
TIntrusivePtr<TOpAggregate> MakeDomainProjection(const TIntrusivePtr<IOperator>& input, const TVector<TInfoUnit>& columns, TPositionHandle pos) {
    Y_ENSURE(!columns.empty(), "Domain of a dependent join cannot be empty");

    TVector<TOpAggregationTraits> traits;
    traits.reserve(columns.size());
    for (const auto& iu : columns) {
        traits.emplace_back(iu, "distinct", iu);
    }
    return MakeIntrusive<TOpAggregate>(input, traits, columns, EOpPhase::Undefined, /*distinctAll=*/true, pos);
}

bool IsNullableIU(const TIntrusivePtr<IOperator>& input, const TInfoUnit& iu) {
    if (!input->Type) {
        return true;
    }
    const auto* columnType = input->GetIUType(iu);
    return !columnType || columnType->IsOptionalOrNull();
}

TVector<std::pair<TInfoUnit, TInfoUnit>> MakeNullSafeJoinKeys(TIntrusivePtr<IOperator>& leftInput, TIntrusivePtr<IOperator>& rightInput,
                                                              const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, TPositionHandle pos, TRBOContext& ctx,
                                                              TPlanProps& props, TInfoUnitSet& usedIUs) {
    TVector<std::pair<TInfoUnit, TInfoUnit>> result;
    result.reserve(joinKeys.size());

    TVector<TMapElement> leftElements;
    TVector<TMapElement> rightElements;

    auto encode = [&](const TInfoUnit& iu, TVector<TMapElement>& elements) {
        auto encodedIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
        // Emulates null == null.
        elements.emplace_back(encodedIU, MakeUnaryCallable("StablePickle", MakeColumnAccess(iu, pos, &ctx.ExprCtx, &props)));
        return encodedIU;
    };

    for (const auto& [leftKey, rightKey] : joinKeys) {
        if (!IsNullableIU(leftInput, leftKey) && !IsNullableIU(rightInput, rightKey)) {
            result.emplace_back(leftKey, rightKey);
            continue;
        }

        result.emplace_back(encode(leftKey, leftElements), encode(rightKey, rightElements));
    }

    if (!leftElements.empty()) {
        leftInput = MakeIntrusive<TOpMap>(leftInput, pos, leftElements);
    }
    if (!rightElements.empty()) {
        rightInput = MakeIntrusive<TOpMap>(rightInput, pos, rightElements);
    }

    return result;
}

bool HasFreeCorrelation(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& correlatedColumns) {
    if (op->Kind == EOperator::AddDependencies) {
        if (ColumnsIntersect(CastOperator<TOpAddDependencies>(op)->Dependencies, correlatedColumns)) {
            return true;
        }
    }

    for (const auto& child : op->Children) {
        if (HasFreeCorrelation(child, correlatedColumns)) {
            return true;
        }
    }

    return false;
}

bool TRewriteDependentJoinToCrossJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TRewriteDependentJoinToCrossJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto depJoinInput = dependentJoin->GetInput();
    // Check that input is dependencies op.
    if (depJoinInput->Kind != EOperator::AddDependencies) {
        return input;
    }

    auto addDependencies = CastOperator<TOpAddDependencies>(depJoinInput);
    if (!IUIsSubset(addDependencies->Dependencies, dependentJoin->Dependencies)) {
        return input;
    }

    if (HasFreeCorrelation(addDependencies->GetInput(), dependentJoin->Dependencies)) {
        return input;
    }

    return MakeCrossJoinWithDomain(dependentJoin, addDependencies->GetInput());
}

bool TRewriteDependentJoinToCrossJoinNoFreeVarsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

// In some cases we can have a situation, when we push dependent join through op, but it does not have a free variables.
// For example for union all we push dependent join for each branch.
TIntrusivePtr<IOperator> TRewriteDependentJoinToCrossJoinNoFreeVarsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                             TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    if (HasFreeCorrelation(dependentJoin->GetInput(), dependentJoin->Dependencies)) {
        return input;
    }

    return MakeCrossJoinWithDomain(dependentJoin, dependentJoin->GetInput());
}

bool TEliminateDependentJoinDomainRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

// We can eliminate dependent join by rewriting it into map -> filter.
TIntrusivePtr<IOperator> TEliminateDependentJoinDomainRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();

    // Dependent join <- Filter <- AddDep.
    if (body->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(body);
    if (filter->GetInput()->Kind != EOperator::AddDependencies) {
        return input;
    }

    auto addDependencies = CastOperator<TOpAddDependencies>(filter->GetInput());
    auto correlatedInput = addDependencies->GetInput();
    if (!IUIsSubset(addDependencies->Dependencies, dependentJoin->Dependencies)) {
        return input;
    }
    if (HasFreeCorrelation(correlatedInput, dependentJoin->Dependencies)) {
        return input;
    }
    const auto innerIUs = correlatedInput->GetOutputIUs();

    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> bindings;
    TVector<TExpression> restConjuncts;
    // Collect eq predicates from each conj.
    for (const auto& conj : filter->GetFilterExpression().SplitConjunct()) {
        std::optional<std::pair<TInfoUnit, TInfoUnit>> binding;

        if (conj.MaybeEquiJoinCondition()) {
            TEquiJoinCondition condition(conj);
            const auto left = condition.GetLeftIU();
            const auto right = condition.GetRightIU();

            if (ContainsInfoUnit(addDependencies->Dependencies, left) && ContainsInfoUnit(innerIUs, right)) {
                binding = std::make_pair(left, right);
            } else if (ContainsInfoUnit(addDependencies->Dependencies, right) && ContainsInfoUnit(innerIUs, left)) {
                binding = std::make_pair(right, left);
            }
        }

        if (binding && bindings.emplace(binding->first, binding->second).second) {
            continue;
        }
        // Keep rest.
        restConjuncts.push_back(conj);
    }

    for (const auto& iu : addDependencies->Dependencies) {
        if (!bindings.contains(iu)) {
            return input;
        }
        if (ContainsInfoUnit(innerIUs, iu)) {
            return input;
        }
    }

    // Here we want to put them into map.
    TVector<TMapElement> bindingElements;
    bindingElements.reserve(addDependencies->Dependencies.size());
    for (const auto& iu : addDependencies->Dependencies) {
        const auto& source = bindings.at(iu);
        bindingElements.emplace_back(iu, MakeColumnAccess(source, filter->Pos, &ctx.ExprCtx, &props));

        // Special case for optional column.
        if (IsNullableIU(correlatedInput, source)) {
            restConjuncts.push_back(MakeUnaryCallable("Exists", MakeColumnAccess(source, filter->Pos, &ctx.ExprCtx, &props)));
        }
    }

    TIntrusivePtr<IOperator> newBody = MakeIntrusive<TOpMap>(correlatedInput, filter->Pos, bindingElements);
    // Keep rest conj in filter, if all domain columns binded into eq prdicates, we can eliminate a filter.
    if (!restConjuncts.empty()) {
        newBody = MakeIntrusive<TOpFilter>(newBody, filter->Pos, MakeConjunction(restConjuncts, props.PgSyntax));
    }

    // If nothing remaining we can return map/filter.
    auto remainingDependencies = MissingDomainColumns(dependentJoin->Dependencies, addDependencies->Dependencies);
    if (remainingDependencies.empty()) {
        return newBody;
    }

    auto domain = dependentJoin->GetDomain();
    if (domain->Kind != EOperator::Aggregate || !CastOperator<TOpAggregate>(domain)->IsDistinctAll()) {
        return input;
    }

    // Reduce domain.
    auto smallerDomain = MakeDomainProjection(CastOperator<TOpAggregate>(domain)->GetInput(), remainingDependencies, domain->Pos);
    return MakeIntrusive<TOpDependentJoin>(smallerDomain, newBody, remainingDependencies, dependentJoin->Pos);
}

bool TPushDependentJoinThroughFilterRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughFilterRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                 TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(body);
    auto newInput = PushInto(dependentJoin, filter->GetInput());
    return MakeIntrusive<TOpFilter>(newInput, filter->Pos, TExpression(filter->GetFilterExpression().GetLambda(), &ctx.ExprCtx, &props));
}

bool TPushDependentJoinThroughMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Map) {
        return input;
    }
    auto map = CastOperator<TOpMap>(body);

    const auto renameSources = map->GetRenameSources();
    for (const auto& iu : dependentJoin->Dependencies) {
        if (renameSources.contains(iu)) {
            return input;
        }
    }

    for (const auto& mapElement : map->GetMapElements()) {
        if (ContainsInfoUnit(dependentJoin->Dependencies, mapElement.GetElementName())) {
            return input;
        }
    }

    auto newInput = PushInto(dependentJoin, map->GetInput());
    return MakeIntrusive<TOpMap>(newInput, map->Pos, map->GetMapElements());
}

bool TPushDependentJoinThroughAggregateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                     TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Aggregate) {
        return input;
    }

    auto aggregate = CastOperator<TOpAggregate>(body);
    Y_ENSURE(aggregate->GetAggregationPhase() == EOpPhase::Undefined);

    auto newKeyColumns = MissingDomainColumns(dependentJoin->Dependencies, aggregate->KeyColumns);
    newKeyColumns.insert(newKeyColumns.end(), aggregate->KeyColumns.begin(), aggregate->KeyColumns.end());

    auto newTraits = aggregate->AggregationTraitsList;
    if (aggregate->IsDistinctAll()) {
        TVector<TInfoUnit> resultColumns;
        resultColumns.reserve(newTraits.size());
        for (const auto& traits : newTraits) {
            resultColumns.push_back(traits.ResultColName);
        }
        TVector<TOpAggregationTraits> domainTraits;
        for (const auto& iu : MissingDomainColumns(dependentJoin->Dependencies, resultColumns)) {
            domainTraits.emplace_back(iu, "distinct", iu);
        }
        domainTraits.insert(domainTraits.end(), newTraits.begin(), newTraits.end());
        newTraits = std::move(domainTraits);
    }

    auto newInput = PushInto(dependentJoin, aggregate->GetInput());
    auto newAggregate =
        MakeIntrusive<TOpAggregate>(newInput, newTraits, newKeyColumns, aggregate->GetAggregationPhase(), aggregate->IsDistinctAll(), aggregate->Pos);

    if (!aggregate->KeyColumns.empty() || aggregate->IsDistinctAll()) {
        return newAggregate;
    }

    // Special case for count.
    TVector<TInfoUnit> countResults;
    for (const auto& traits : newTraits) {
        if (traits.AggFunction == "count") {
            countResults.push_back(traits.ResultColName);
        }
    }

    if (countResults.empty()) {
        return newAggregate;
    }

    return RestoreEmptyGroupCounts(dependentJoin, newAggregate, countResults, ctx, props);
}

bool TPushDependentJoinThroughUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                   TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::UnionAll) {
        return input;
    }
    auto unionAll = CastOperator<TOpUnionAll>(body);

    if (ColumnsIntersect(dependentJoin->Dependencies, unionAll->Columns)) {
        return input;
    }

    auto newColumns = dependentJoin->Dependencies;
    newColumns.insert(newColumns.end(), unionAll->Columns.begin(), unionAll->Columns.end());

    // Push on each side.
    TVector<TIntrusivePtr<IOperator>> newInputs;
    newInputs.reserve(unionAll->Children.size());
    for (const auto& child : unionAll->Children) {
        newInputs.push_back(PushInto(dependentJoin, child));
    }

    return MakeIntrusive<TOpUnionAll>(newInputs, unionAll->Pos, newColumns, unionAll->Ordered);
}

bool TPushDependentJoinThroughJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Join) {
        return input;
    }
    auto join = CastOperator<TOpJoin>(body);
    const auto& dependencies = dependentJoin->Dependencies;

    const bool leftCorrelated = HasFreeCorrelation(join->GetLeftInput(), dependencies);
    const bool rightCorrelated = HasFreeCorrelation(join->GetRightInput(), dependencies);

    if (!leftCorrelated && !rightCorrelated) {
        return input;
    }

    const auto joinKind = GetValidJoinKind(join->JoinKind);
    const bool innerLike = joinKind == "Inner" || joinKind == "Cross";

    // Here we want to push the dependent join on the side where we have a free variables.
    if (leftCorrelated && !rightCorrelated) {
        if (!JoinOutputsLeft(joinKind)) {
            return input;
        }
        auto newLeft = PushInto(dependentJoin, join->GetLeftInput());
        return MakeIntrusive<TOpJoin>(newLeft, join->GetRightInput(), join->Pos, join->JoinKind, join->JoinKeys, join->JoinFilters);
    }

    if (!leftCorrelated && rightCorrelated) {
        if (!innerLike) {
            return input;
        }
        auto newRight = PushInto(dependentJoin, join->GetRightInput());
        return MakeIntrusive<TOpJoin>(join->GetLeftInput(), newRight, join->Pos, join->JoinKind, join->JoinKeys, join->JoinFilters);
    }

    // I dont know about other kinds.
    if (!innerLike && joinKind != "Left" && joinKind != "LeftSemi" && joinKind != "LeftOnly") {
        return input;
    }

    TIntrusivePtr<IOperator> newLeft = PushInto(dependentJoin, join->GetLeftInput());
    TIntrusivePtr<IOperator> newRight = PushInto(dependentJoin, join->GetRightInput());

    TInfoUnitSet usedIUs;
    NMapRenames::AddUsedIUs(usedIUs, newLeft->GetOutputIUs());
    NMapRenames::AddUsedIUs(usedIUs, newRight->GetOutputIUs());
    const auto rightRenames = NMapRenames::MakeRenameMap(dependencies, props.InternalVarIdx, usedIUs);

    // Add a domain keys if both side a correlated.
    TVector<std::pair<TInfoUnit, TInfoUnit>> domainKeys;
    for (const auto& iu : dependencies) {
        domainKeys.emplace_back(iu, iu);
    }
    domainKeys = MakeNullSafeJoinKeys(newLeft, newRight, domainKeys, join->Pos, ctx, props, usedIUs);

    auto joinKeys = join->JoinKeys;
    joinKeys.insert(joinKeys.end(), domainKeys.begin(), domainKeys.end());

    // rewrite cross to inner because it has a join keys based on domain.
    const TString newJoinKind = joinKind == "Cross" ? "Inner" : joinKind;

    return NMapRenames::MakeJoinWithRightRenames(newLeft, newRight, join->Pos, newJoinKind, joinKeys, join->JoinFilters, rightRenames, ctx.ExprCtx, props);
}

bool TDependentJoinNotSupportedRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

// We should fail if we cannot push dependent join or rewrite it or eliminate it.
TIntrusivePtr<IOperator> TDependentJoinNotSupportedRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();

    // Nested one.
    if (HasOperatorBelow(body, EOperator::DependentJoin)) {
        return input;
    }

    Y_ENSURE(false, "Cannot decorrelate the subquery, correlation cannot be pushed through " << body->GetExplainName());
    return input;
}

} // namespace NKqp
} // namespace NKikimr
